import sys
import grpc
import cloudsecurity_pb2
import cloudsecurity_pb2_grpc


def build_message(msg_cls, values: dict):
    """Build a protobuf message `msg_cls` by setting only fields that exist.
    This allows the client to work with slightly different proto versions
    where a field may be named `email` or `login`.
    """
    msg = msg_cls()
    # fields_by_name available on the descriptor
    available = set(f.name for f in msg.DESCRIPTOR.fields)
    for k, v in values.items():
        if k in available:
            setattr(msg, k, v)
    return msg


def run(command, *args):
    with grpc.insecure_channel('localhost:51234') as channel:
        stub = cloudsecurity_pb2_grpc.UserServiceStub(channel)

        if command == 'signup':
            # Expect: signup <username> <email> <password>
            if len(args) < 3:
                print('Usage: signup <username> <email> <password>')
                return
            username, email, password = args[0], args[1], args[2]
            req = build_message(cloudsecurity_pb2.SignupRequest, {
                'username': username,
                'email': email,
                'login': username,
                'password': password,
            })
            resp = stub.Signup(req)
            # Print common fields if present
            print('Signup response:')
            for attr in ('success', 'message', 'otp_required', 'temp_token', 'auth_token'):
                if hasattr(resp, attr):
                    print(f'  {attr}:', getattr(resp, attr))

        elif command == 'login':
            # Expect: login <login_or_email> <password>
            if len(args) < 2:
                print('Usage: login <login_or_email> <password>')
                return
            login_val, password = args[0], args[1]
            req = build_message(cloudsecurity_pb2.LoginRequest, {
                'login': login_val,
                'email': login_val,
                'password': password,
            })
            resp = stub.Login(req)
            print('Login response:')
            for attr in ('success', 'message', 'otp_required', 'temp_token', 'auth_token'):
                if hasattr(resp, attr):
                    print(f'  {attr}:', getattr(resp, attr))

        elif command == 'verify':
            # Expect: verify <email> <otp>
            if len(args) < 2:
                print('Usage: verify <email> <otp>')
                return
            email, otp = args[0], args[1]
            req = build_message(cloudsecurity_pb2.OtpRequest, {
                'email': email,
                'otp': otp,
            })
            resp = stub.VerifyOTP(req)
            print('Verify OTP response:')
            for attr in ('success', 'message', 'auth_token'):
                if hasattr(resp, attr):
                    print(f'  {attr}:', getattr(resp, attr))

        else:
            print('Invalid command. Use "signup", "login", or "verify"')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: client.py <signup|login|verify> [args...]')
        sys.exit(1)
    cmd = sys.argv[1]
    run(cmd, *sys.argv[2:])