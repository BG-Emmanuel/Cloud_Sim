import grpc
from concurrent import futures
import json
import requests
import time
from requests import exceptions as req_exceptions
import cloudsecurity_pb2
import cloudsecurity_pb2_grpc
import traceback
from utils import send_otp


def load_api_key():
    """Read the Firebase API key from `google-services.json` if present."""
    try:
        with open('google-services.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            # navigate to client[0].api_key[0].current_key
            clients = data.get('client', [])
            if clients:
                api_keys = clients[0].get('api_key', [])
                if api_keys:
                    return api_keys[0].get('current_key')
    except FileNotFoundError:
        pass
    return None


class UserServiceSkeleton(cloudsecurity_pb2_grpc.UserServiceServicer):
    def __init__(self):
        self.api_key = load_api_key()

    def _post_with_retries(self, url, payload, retries: int = 3, timeout: int = 15):
        """Post JSON to `url` with simple retry/backoff for transient errors.
        Returns a successful `requests.Response` or raises the last exception.
        """
        backoff = 1
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                resp = requests.post(url, json=payload, timeout=timeout)
                resp.raise_for_status()
                return resp
            except req_exceptions.Timeout as e:
                last_exc = e
                if attempt == retries:
                    raise
                time.sleep(backoff)
                backoff *= 2
            except req_exceptions.RequestException as e:
                # For connection errors or HTTP errors, propagate immediately
                raise
        # If we exit the loop without success, raise the last exception
        if last_exc:
            raise last_exc
        raise RuntimeError('Unexpected failure in _post_with_retries')

    def _get_field(self, msg, *names):
        """Return the first available field value from `msg` for any of `names`.
        This makes the server tolerant to small proto variations (e.g. `email` vs `login`).
        """
        for n in names:
            if hasattr(msg, n):
                try:
                    val = getattr(msg, n)
                    if val:
                        return val
                except Exception:
                    continue
        return ''

    def Signup(self, request, context) -> cloudsecurity_pb2.AuthResponse:
        """Create a new Firebase user using the Identity Toolkit REST API."""
        try:
            print('Signup request received:', request)
            if not self.api_key:
                return cloudsecurity_pb2.AuthResponse(success=False, message='No Firebase API key configured')

            email = self._get_field(request, 'email', 'login')
            password = self._get_field(request, 'password')

            url = f'https://identitytoolkit.googleapis.com/v1/accounts:signUp?key={self.api_key}'
            payload = {
                'email': email,
                'password': password,
                'returnSecureToken': True,
            }
            r = self._post_with_retries(url, payload, retries=3, timeout=15)
            data = r.json()
            # data contains idToken, email, refreshToken, expiresIn, localId
            return cloudsecurity_pb2.AuthResponse(
                success=True,
                message='Signup successful',
                otp_required=False,
                temp_token=data.get('idToken', ''),
                auth_token=data.get('idToken', ''),
            )
        except req_exceptions.Timeout as e:
            msg = 'Request to Firebase timed out. Check network connectivity and try again.'
            print('Signup timeout:', e)
            return cloudsecurity_pb2.AuthResponse(success=False, message=msg)
        except req_exceptions.RequestException as e:
            # includes HTTPError and ConnectionError
            try:
                body = e.response.json() if getattr(e, 'response', None) is not None else None
            except Exception:
                body = None
            print('Signup request exception:', e, 'body:', body)
            return cloudsecurity_pb2.AuthResponse(success=False, message=f'Firebase request failed: {e}')
        except Exception as e:
            tb = traceback.format_exc()
            print('Signup handler exception:', tb)
            return cloudsecurity_pb2.AuthResponse(success=False, message=f'Internal server error: {e}')

    def Login(self, request, context) -> cloudsecurity_pb2.AuthResponse:
        """Authenticate a user via Firebase REST signInWithPassword endpoint."""
        try:
            print('Login request received:', request)
            if not self.api_key:
                return cloudsecurity_pb2.AuthResponse(success=False, message='No Firebase API key configured')

            email = self._get_field(request, 'email', 'login')
            password = self._get_field(request, 'password')

            url = f'https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={self.api_key}'
            payload = {
                'email': email,
                'password': password,
                'returnSecureToken': True,
            }
            r = self._post_with_retries(url, payload, retries=3, timeout=15)
            data = r.json()
            return cloudsecurity_pb2.AuthResponse(
                success=True,
                message='Login successful',
                otp_required=False,
                temp_token=data.get('idToken', ''),
                auth_token=data.get('idToken', ''),
            )
        except req_exceptions.Timeout as e:
            msg = 'Request to Firebase timed out. Check network connectivity and try again.'
            print('Login timeout:', e)
            return cloudsecurity_pb2.AuthResponse(success=False, message=msg)
        except req_exceptions.RequestException as e:
            try:
                body = e.response.json() if getattr(e, 'response', None) is not None else None
            except Exception:
                body = None
            print('Login request exception:', e, 'body:', body)
            return cloudsecurity_pb2.AuthResponse(success=False, message=f'Firebase request failed: {e}')
        except Exception as e:
            tb = traceback.format_exc()
            print('Login handler exception:', tb)
            return cloudsecurity_pb2.AuthResponse(success=False, message=f'Internal server error: {e}')

    def VerifyOTP(self, request, context) -> cloudsecurity_pb2.VerifyOTPResponse:
        # OTP is not handled by Firebase directly in this implementation.
        # You can implement an OTP store and verification here. For now return not implemented.
        return cloudsecurity_pb2.VerifyOTPResponse(success=False, message='OTP verification not implemented')


def run():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cloudsecurity_pb2_grpc.add_UserServiceServicer_to_server(UserServiceSkeleton(), server)
    server.add_insecure_port('[::]:51234')
    print('Starting Server on port 51234 ............', end='')
    server.start()
    print('[OK]')
    server.wait_for_termination()


if __name__ == '__main__':
    run()