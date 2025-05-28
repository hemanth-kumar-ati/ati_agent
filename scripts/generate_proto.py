import subprocess
from pathlib import Path

def generate_proto():
    """Generate Python code from protobuf definition"""
    project_root = Path(__file__).parent.parent
    proto_dir = project_root / 'bridge_service' / 'proto'
    proto_file = proto_dir / 'employee.proto'
    
    # Create output directory if it doesn't exist
    proto_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate Python code
    cmd = [
        'protoc',
        f'--proto_path={proto_dir}',
        f'--python_out={proto_dir}',
        str(proto_file)
    ]
    
    print(f"Generating Python code from {proto_file}")
    subprocess.run(cmd, check=True)
    print(f"Generated Python code in {proto_dir}")

if __name__ == "__main__":
    generate_proto() 