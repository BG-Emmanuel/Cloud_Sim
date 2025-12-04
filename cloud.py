import grpc
from concurrent import futures
import json
import requests
import time
from requests import exceptions as req_exceptions
import cloudsecurity_pb2
import cloudsecurity_pb2_grpc
import traceback
from utils import send_otp, generate_otp


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
        self.otp_store = {}  # In-memory OTP storage
        self.verified_users = {}  # Store verified credentials temporarily

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
        """Create a new Firebase user - requires OTP verification before completing."""
        try:
            print('Signup request received:', request)
            if not self.api_key:
                return cloudsecurity_pb2.AuthResponse(success=False, message='No Firebase API key configured')

            email = self._get_field(request, 'email', 'login')
            password = self._get_field(request, 'password')

            # Generate and send OTP FIRST before creating Firebase account
            otp = send_otp(email)
            if not otp or not otp.isdigit():
                return cloudsecurity_pb2.AuthResponse(
                    success=False,
                    message=f'Failed to send OTP: {otp}'
                )
            
            # Store OTP and credentials temporarily
            self.otp_store[email] = {
                'otp': otp,
                'password': password,
                'action': 'signup',
                'timestamp': time.time()
            }
            
            print(f'OTP sent to {email} for signup')
            
            return cloudsecurity_pb2.AuthResponse(
                success=True,
                message=f'OTP sent to {email}. Please verify OTP to complete signup.',
                otp_required=True,
                temp_token='',
                auth_token='',
            )
        except Exception as e:
            tb = traceback.format_exc()
            print('Signup handler exception:', tb)
            return cloudsecurity_pb2.AuthResponse(success=False, message=f'Internal server error: {e}')

    def Login(self, request, context) -> cloudsecurity_pb2.AuthResponse:
        """Authenticate a user - requires OTP verification before returning token."""
        try:
            print('Login request received:', request)
            if not self.api_key:
                return cloudsecurity_pb2.AuthResponse(success=False, message='No Firebase API key configured')

            email = self._get_field(request, 'email', 'login')
            password = self._get_field(request, 'password')

            # First verify credentials with Firebase
            url = f'https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={self.api_key}'
            payload = {
                'email': email,
                'password': password,
                'returnSecureToken': True,
            }
            
            try:
                r = self._post_with_retries(url, payload, retries=3, timeout=15)
                data = r.json()
            except req_exceptions.RequestException as e:
                try:
                    body = e.response.json() if getattr(e, 'response', None) is not None else None
                except Exception:
                    body = None
                print('Login Firebase check failed:', e, 'body:', body)
                return cloudsecurity_pb2.AuthResponse(
                    success=False, 
                    message='Invalid email or password'
                )
            
            # Credentials are valid - now send OTP
            otp = send_otp(email)
            if not otp or not otp.isdigit():
                return cloudsecurity_pb2.AuthResponse(
                    success=False,
                    message=f'Authentication successful but failed to send OTP: {otp}'
                )
            
            # Store OTP and Firebase token temporarily
            self.otp_store[email] = {
                'otp': otp,
                'token': data.get('idToken', ''),
                'action': 'login',
                'timestamp': time.time()
            }
            
            print(f'OTP sent to {email} for login')
            
            return cloudsecurity_pb2.AuthResponse(
                success=True,
                message=f'Credentials verified. OTP sent to {email}. Please verify OTP to complete login.',
                otp_required=True,
                temp_token='',
                auth_token='',
            )
        except req_exceptions.Timeout as e:
            msg = 'Request to Firebase timed out. Check network connectivity and try again.'
            print('Login timeout:', e)
            return cloudsecurity_pb2.AuthResponse(success=False, message=msg)
        except Exception as e:
            tb = traceback.format_exc()
            print('Login handler exception:', tb)
            return cloudsecurity_pb2.AuthResponse(success=False, message=f'Internal server error: {e}')

    def VerifyOTP(self, request, context) -> cloudsecurity_pb2.VerifyOTPResponse:
        """Verify the OTP and complete signup/login."""
        try:
            email = self._get_field(request, 'email')
            otp = self._get_field(request, 'otp')
            
            print(f'VerifyOTP request received for email: {email}')
            
            if email not in self.otp_store:
                return cloudsecurity_pb2.VerifyOTPResponse(
                    success=False,
                    message='No OTP found for this email. Please signup or login first.',
                    auth_token=''
                )
            
            stored = self.otp_store[email]
            
            # Check if OTP expired (5 minutes = 300 seconds)
            if time.time() - stored['timestamp'] > 300:
                del self.otp_store[email]
                return cloudsecurity_pb2.VerifyOTPResponse(
                    success=False,
                    message='OTP expired. Please signup or login again.',
                    auth_token=''
                )
            
            # Verify OTP
            if stored['otp'] != otp:
                print(f'Invalid OTP for {email}. Expected: {stored["otp"]}, Got: {otp}')
                return cloudsecurity_pb2.VerifyOTPResponse(
                    success=False,
                    message='Invalid OTP. Please try again.',
                    auth_token=''
                )
            
            # OTP is valid - now complete the action
            action = stored.get('action', 'login')
            
            if action == 'signup':
                # Complete Firebase signup
                password = stored.get('password')
                url = f'https://identitytoolkit.googleapis.com/v1/accounts:signUp?key={self.api_key}'
                payload = {
                    'email': email,
                    'password': password,
                    'returnSecureToken': True,
                }
                try:
                    r = self._post_with_retries(url, payload, retries=3, timeout=15)
                    data = r.json()
                    auth_token = data.get('idToken', '')
                    del self.otp_store[email]
                    print(f'Signup completed successfully for {email}')
                    return cloudsecurity_pb2.VerifyOTPResponse(
                        success=True,
                        message='OTP verified. Signup completed successfully!',
                        auth_token=auth_token
                    )
                except req_exceptions.RequestException as e:
                    try:
                        body = e.response.json() if getattr(e, 'response', None) is not None else None
                    except Exception:
                        body = None
                    print('Signup after OTP verification failed:', e, 'body:', body)
                    del self.otp_store[email]
                    if body:
                        print('Firebase error details:', json.dumps(body, indent=2))
                    return cloudsecurity_pb2.VerifyOTPResponse(
                        success=False,
                        message=f'OTP verified but signup failed: {e}',
                        auth_token=''
                    )
            
            else:  # action == 'login'
                # Return the stored auth token
                auth_token = stored.get('token', '')
                del self.otp_store[email]
                print(f'Login completed successfully for {email}')
                return cloudsecurity_pb2.VerifyOTPResponse(
                    success=True,
                    message='OTP verified. Login successful!',
                    auth_token=auth_token
                )
                
        except Exception as e:
            tb = traceback.format_exc()
            print('VerifyOTP exception:', tb)
            return cloudsecurity_pb2.VerifyOTPResponse(
                success=False,
                message=f'Error verifying OTP: {e}',
                auth_token=''
            )


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