import bcrypt
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from params import from_email, from_password   # keep your credentials in params.py


# -----------------------------
# Password hashing
# -----------------------------
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'),
                         bcrypt.gensalt()).decode('utf-8')


# -----------------------------
# OTP generation
# -----------------------------
def generate_otp() -> str:
    return str(random.randint(100000, 999999))


# -----------------------------
# Send OTP via Gmail
# -----------------------------
def send_otp(to_email: str) -> str:
    otp = generate_otp()

    subject = "Your OTP Code for the Cloud Security Simulator"
    body = f"Your OTP code is: {otp}"

    # Build email
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            # Helpful debug output while developing; set to 0 to silence
            server.set_debuglevel(0)
            server.ehlo()
            print("Starting TLS session...")
            server.starttls()
            server.ehlo()
            print("Logging in...")

            # Prefer explicit from_password; otherwise fall back to app_password
            pw = (from_password or '').strip()
            if not pw:
                # Remove spaces which sometimes appear when copying app passwords
                pw = (app_password or '').replace(' ', '').strip()

            if not pw:
                raise ValueError('No email password configured. Set `from_password` or `app_password` in params.py')

            server.login(from_email, pw)
            print("Sending email...")
            server.sendmail(from_email, [to_email], msg.as_string())
            print(f"OTP sent to {to_email} successfully!")
            return otp   # return OTP for later verification
    except Exception as e:
        err = f"Failed to send email: {e}"
        print(err)
        return err


# -----------------------------
# Main program
# -----------------------------
if __name__ == '__main__':
    # --- Optional: migrate plain credentials -> hashed file (keeps previous behavior) ---
    try:
        credentials = {}
        file_path = 'ids'   # file containing username,password pairs
        with open(file_path, 'r') as file:
            for line in file:
                username, password = line.strip().split(',')
                credentials[username] = password

        with open('credentials', 'w') as file:
            for username, password in credentials.items():
                file.write(f'{username},{hash_password(password)}\n')
    except FileNotFoundError:
        # `ids` file not present is fine; continue to OTP sending
        pass

    # Send an OTP when running this module directly.
    recipient = 'neyma7704@gmail.com'
    result = send_otp(recipient)
    if isinstance(result, str) and result.isdigit():
        print(f'Generated OTP: {result}')
    else:
        print(f'Send result: {result}')