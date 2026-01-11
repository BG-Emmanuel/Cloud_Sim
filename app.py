# ==============================================================================
# FILE: app.py (UPDATED)
# Location: backend/app.py
# ==============================================================================

from flask import Flask, request, jsonify, send_file, send_from_directory
from flask_cors import CORS
import grpc
import cloudsecurity_pb2
import cloudsecurity_pb2_grpc
import os
import json
from werkzeug.utils import secure_filename
import hashlib
from datetime import datetime

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

# Configuration
GRPC_SERVER = 'localhost:51234'
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'zip', 'rar'}
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB

# Create upload folder if it doesn't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# File metadata storage (in production, use a database)
FILE_METADATA_PATH = 'file_metadata.json'

def load_file_metadata():
    if os.path.exists(FILE_METADATA_PATH):
        with open(FILE_METADATA_PATH, 'r') as f:
            return json.load(f)
    return {}

def save_file_metadata(metadata):
    with open(FILE_METADATA_PATH, 'w') as f:
        json.dump(metadata, f, indent=2)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_file_hash(filepath):
    """Generate MD5 hash of file"""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def get_grpc_stub():
    """Create a gRPC stub for UserService"""
    channel = grpc.insecure_channel(GRPC_SERVER)
    return cloudsecurity_pb2_grpc.UserServiceStub(channel)

def build_message(msg_cls, values: dict):
    """Build a protobuf message by setting only fields that exist"""
    msg = msg_cls()
    available = set(f.name for f in msg.DESCRIPTOR.fields)
    for k, v in values.items():
        if k in available:
            setattr(msg, k, v)
    return msg

# ==================== WEB ROUTES ====================

@app.route('/')
def index():
    """Serve the login page"""
    return send_file('index.html')

@app.route('/dashboard')
def dashboard():
    """Serve the dashboard page"""
    return send_file('dashboard.html')

# ==================== AUTH ROUTES ====================

@app.route('/api/signup', methods=['POST'])
def signup():
    """Handle signup requests"""
    try:
        data = request.get_json()
        username = data.get('username', '')
        email = data.get('email', '')
        password = data.get('password', '')
        
        if not username or not email or not password:
            return jsonify({
                'success': False,
                'message': 'Username, email, and password are required'
            }), 400
        
        stub = get_grpc_stub()
        req = build_message(cloudsecurity_pb2.SignupRequest, {
            'username': username,
            'email': email,
            'login': username,
            'password': password,
        })
        
        response = stub.Signup(req)
        
        return jsonify({
            'success': response.success,
            'message': response.message,
            'otp_required': getattr(response, 'otp_required', False),
            'temp_token': getattr(response, 'temp_token', ''),
            'auth_token': getattr(response, 'auth_token', '')
        })
        
    except grpc.RpcError as e:
        return jsonify({
            'success': False,
            'message': f'Service error: {e.details()}'
        }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Server error: {str(e)}'
        }), 500

@app.route('/api/login', methods=['POST'])
def login():
    """Handle login requests"""
    try:
        data = request.get_json()
        email = data.get('email', '')
        password = data.get('password', '')
        
        if not email or not password:
            return jsonify({
                'success': False,
                'message': 'Email and password are required'
            }), 400
        
        stub = get_grpc_stub()
        req = build_message(cloudsecurity_pb2.LoginRequest, {
            'login': email,
            'email': email,
            'password': password,
        })
        
        response = stub.Login(req)
        
        return jsonify({
            'success': response.success,
            'message': response.message,
            'otp_required': getattr(response, 'otp_required', False),
            'temp_token': getattr(response, 'temp_token', ''),
            'auth_token': getattr(response, 'auth_token', '')
        })
        
    except grpc.RpcError as e:
        return jsonify({
            'success': False,
            'message': f'Service error: {e.details()}'
        }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Server error: {str(e)}'
        }), 500

@app.route('/api/verify-otp', methods=['POST'])
def verify_otp():
    """Handle OTP verification requests"""
    try:
        data = request.get_json()
        email = data.get('email', '')
        otp = data.get('otp', '')
        temp_token = data.get('temp_token', '')
        
        if not email or not otp:
            return jsonify({
                'success': False,
                'message': 'Email and OTP are required'
            }), 400
        
        stub = get_grpc_stub()
        verify_cls = None
        for name in ('VerifyOTPRequest', 'OtpRequest'):
            verify_cls = getattr(cloudsecurity_pb2, name, None)
            if verify_cls is not None:
                break
        
        if verify_cls is None:
            return jsonify({
                'success': False,
                'message': 'OTP verification not available'
            }), 500
        
        req = build_message(verify_cls, {
            'email': email,
            'login': email,
            'otp': otp,
            'temp_token': temp_token,
        })
        
        response = stub.VerifyOTP(req)
        
        return jsonify({
            'success': response.success,
            'message': response.message,
            'auth_token': getattr(response, 'auth_token', '')
        })
        
    except grpc.RpcError as e:
        return jsonify({
            'success': False,
            'message': f'Service error: {e.details()}'
        }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Server error: {str(e)}'
        }), 500

# ==================== FILE MANAGEMENT ROUTES ====================

@app.route('/api/files/upload', methods=['POST'])
def upload_file():
    """Handle file upload"""
    try:
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'message': 'No file provided'
            }), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({
                'success': False,
                'message': 'No file selected'
            }), 400
        
        if not allowed_file(file.filename):
            return jsonify({
                'success': False,
                'message': 'File type not allowed'
            }), 400
        
        # Secure the filename
        filename = secure_filename(file.filename)
        
        # Generate unique filename if file exists
        base_name, extension = os.path.splitext(filename)
        counter = 1
        while os.path.exists(os.path.join(UPLOAD_FOLDER, filename)):
            filename = f"{base_name}_{counter}{extension}"
            counter += 1
        
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)
        
        # Get file info
        file_size = os.path.getsize(filepath)
        file_hash = get_file_hash(filepath)
        
        # Store metadata
        metadata = load_file_metadata()
        file_id = file_hash[:16]  # Use first 16 chars of hash as ID
        
        metadata[file_id] = {
            'id': file_id,
            'name': filename,
            'original_name': file.filename,
            'size': file_size,
            'type': extension[1:] if extension else 'unknown',
            'hash': file_hash,
            'uploaded': datetime.now().isoformat(),
            'status': 'completed'
        }
        
        save_file_metadata(metadata)
        
        return jsonify({
            'success': True,
            'message': 'File uploaded successfully',
            'file': metadata[file_id]
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Upload error: {str(e)}'
        }), 500

@app.route('/api/files/list', methods=['GET'])
def list_files():
    """List all uploaded files"""
    try:
        metadata = load_file_metadata()
        files = list(metadata.values())
        
        # Calculate total storage used
        total_size = sum(f['size'] for f in files)
        
        return jsonify({
            'success': True,
            'files': files,
            'total_files': len(files),
            'total_size': total_size
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error listing files: {str(e)}'
        }), 500

@app.route('/api/files/download/<file_id>', methods=['GET'])
def download_file(file_id):
    """Download a file"""
    try:
        metadata = load_file_metadata()
        
        if file_id not in metadata:
            return jsonify({
                'success': False,
                'message': 'File not found'
            }), 404
        
        file_info = metadata[file_id]
        filepath = os.path.join(UPLOAD_FOLDER, file_info['name'])
        
        if not os.path.exists(filepath):
            return jsonify({
                'success': False,
                'message': 'File not found on disk'
            }), 404
        
        return send_from_directory(
            UPLOAD_FOLDER,
            file_info['name'],
            as_attachment=True,
            download_name=file_info['original_name']
        )
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Download error: {str(e)}'
        }), 500

@app.route('/api/files/delete/<file_id>', methods=['DELETE'])
def delete_file(file_id):
    """Delete a file"""
    try:
        metadata = load_file_metadata()
        
        if file_id not in metadata:
            return jsonify({
                'success': False,
                'message': 'File not found'
            }), 404
        
        file_info = metadata[file_id]
        filepath = os.path.join(UPLOAD_FOLDER, file_info['name'])
        
        # Delete file from disk
        if os.path.exists(filepath):
            os.remove(filepath)
        
        # Remove from metadata
        del metadata[file_id]
        save_file_metadata(metadata)
        
        return jsonify({
            'success': True,
            'message': 'File deleted successfully'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Delete error: {str(e)}'
        }), 500

@app.route('/api/storage/stats', methods=['GET'])
def storage_stats():
    """Get storage statistics"""
    try:
        metadata = load_file_metadata()
        files = list(metadata.values())
        
        total_size = sum(f['size'] for f in files)
        total_capacity = 100 * 1024 * 1024 * 1024  # 100GB in bytes
        
        # Group files by type
        file_types = {}
        for f in files:
            ftype = f['type']
            if ftype not in file_types:
                file_types[ftype] = {'count': 0, 'size': 0}
            file_types[ftype]['count'] += 1
            file_types[ftype]['size'] += f['size']
        
        return jsonify({
            'success': True,
            'stats': {
                'total_files': len(files),
                'total_size': total_size,
                'total_capacity': total_capacity,
                'used_percent': (total_size / total_capacity) * 100,
                'file_types': file_types
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error getting stats: {str(e)}'
        }), 500

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    try:
        stub = get_grpc_stub()
        return jsonify({
            'status': 'healthy',
            'grpc_server': GRPC_SERVER
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 503

if __name__ == '__main__':
    if not os.path.exists('index.html'):
        print("Warning: index.html not found in current directory")
    
    print(f"Flask server starting...")
    print(f"Upload folder: {os.path.abspath(UPLOAD_FOLDER)}")
    print(f"Connecting to gRPC server at {GRPC_SERVER}")
    print(f"Web UI available at http://localhost:5000")
    print(f"Dashboard at http://localhost:5000/dashboard")
    print(f"API endpoints at http://localhost:5000/api/*")
    
    app.run(host='0.0.0.0', port=5000, debug=True)