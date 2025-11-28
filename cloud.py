import grpc
from concurrent import futures
import json
import requests
import cloudsecurity_pb2
import cloudsecurity_pb2_grpc
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

    def Signup(self, request, context) -> cloudsecurity_pb2.AuthResponse:
        """Create a new Firebase user using the Identity Toolkit REST API."""
        if not self.api_key:
            return cloudsecurity_pb2.AuthResponse(success=False, message='No Firebase API key configured')

        url = f'https://identitytoolkit.googleapis.com/v1/accounts:signUp?key={self.api_key}'
        payload = {
            'email': request.email,
            'password': request.password,
            'returnSecureToken': True,
        }
        try:
            r = requests.post(url, json=payload, timeout=10)
            r.raise_for_status()
            data = r.json()
            # data contains idToken, email, refreshToken, expiresIn, localId
            return cloudsecurity_pb2.AuthResponse(
                success=True,
                message='Signup successful',
                otp_required=False,
                temp_token=data.get('idToken', ''),
                auth_token=data.get('idToken', ''),
            )
        except requests.HTTPError as e:
            err = r.json() if 'r' in locals() else {'error': str(e)}
            msg = err.get('error', err)
            return cloudsecurity_pb2.AuthResponse(success=False, message=str(msg))
        except Exception as e:
            return cloudsecurity_pb2.AuthResponse(success=False, message=str(e))

    def Login(self, request, context) -> cloudsecurity_pb2.AuthResponse:
        """Authenticate a user via Firebase REST signInWithPassword endpoint."""
        if not self.api_key:
            return cloudsecurity_pb2.AuthResponse(success=False, message='No Firebase API key configured')

        url = f'https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={self.api_key}'
        payload = {
            'email': request.email,
            'password': request.password,
            'returnSecureToken': True,
        }
        try:
            r = requests.post(url, json=payload, timeout=10)
            r.raise_for_status()
            data = r.json()
            return cloudsecurity_pb2.AuthResponse(
                success=True,
                message='Login successful',
                otp_required=False,
                temp_token=data.get('idToken', ''),
                auth_token=data.get('idToken', ''),
            )
        except requests.HTTPError as e:
            err = r.json() if 'r' in locals() else {'error': str(e)}
            msg = err.get('error', err)
            return cloudsecurity_pb2.AuthResponse(success=False, message=str(msg))
        except Exception as e:
            return cloudsecurity_pb2.AuthResponse(success=False, message=str(e))

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