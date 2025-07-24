#!/usr/bin/env python3
"""
Build script to create standalone executable for SQL Anywhere Sync Tool
This script uses PyInstaller to create a single executable file.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def check_pyinstaller():
    """Check if PyInstaller is installed"""
    try:
        import PyInstaller
        print("✅ PyInstaller is available")
        return True
    except ImportError:
        print("❌ PyInstaller not found. Installing...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])
            print("✅ PyInstaller installed successfully")
            return True
        except subprocess.CalledProcessError:
            print("❌ Failed to install PyInstaller")
            return False

def check_dependencies():
    """Check if required dependencies are installed"""
    dependencies = ["pyodbc", "requests"]
    missing = []
    
    for dep in dependencies:
        try:
            __import__(dep)
            print(f"✅ {dep} is available")
        except ImportError:
            missing.append(dep)
            print(f"❌ {dep} not found")
    
    if missing:
        print(f"Installing missing dependencies: {', '.join(missing)}")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
            print("✅ Dependencies installed successfully")
            return True
        except subprocess.CalledProcessError:
            print("❌ Failed to install dependencies")
            return False
    
    return True

def create_build_directory():
    """Create and prepare build directory"""
    build_dir = Path("dist")
    if build_dir.exists():
        print("🧹 Cleaning existing build directory...")
        shutil.rmtree(build_dir)
    
    build_dir.mkdir(exist_ok=True)
    print("📁 Build directory created")

def build_executable():
    """Build the executable using PyInstaller"""
    print("🔨 Building executable...")
    
    # PyInstaller command
    cmd = [
        "pyinstaller",
        "--onefile",                    # Create single executable
        "--console",                    # Keep console window
        "--name", "SyncTool",          # Executable name
        "--clean",                      # Clean cache
        "--noconfirm",                 # Don't ask for confirmation
        "--distpath", "dist",          # Output directory
        "--workpath", "build",         # Work directory
        "--specpath", ".",             # Spec file location
        "sync.py"                      # Main script
    ]
    
    try:
        subprocess.check_call(cmd)
        print("✅ Executable built successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Build failed: {e}")
        return False

def create_deployment_package():
    """Create deployment package with executable and config"""
    print("📦 Creating deployment package...")
    
    # Create deployment directory
    deploy_dir = Path("SQLAnywhereSync")
    if deploy_dir.exists():
        shutil.rmtree(deploy_dir)
    
    deploy_dir.mkdir()
    
    # Copy executable
    exe_source = Path("dist/SyncTool.exe")
    if not exe_source.exists():
        exe_source = Path("dist/SyncTool")  # Linux/Mac
    
    if exe_source.exists():
        shutil.copy2(exe_source, deploy_dir / exe_source.name)
        print("✅ Executable copied to deployment package")
    else:
        print("❌ Executable not found")
        return False
    
    # Copy config file
    config_source = Path("config.json")
    if config_source.exists():
        shutil.copy2(config_source, deploy_dir)
        print("✅ Config file copied to deployment package")
    
    # Create batch file for Windows
    batch_content = """@echo off
echo Starting SQL Anywhere Sync Tool...
echo.
SyncTool.exe
pause
"""
    
    with open(deploy_dir / "sync.bat", "w") as f:
        f.write(batch_content)
    print("✅ Batch file created")
    
    # Create shell script for Linux/Mac
    shell_content = """#!/bin/bash
echo "Starting SQL Anywhere Sync Tool..."
echo
./SyncTool
read -p "Press Enter to exit..."
"""
    
    shell_script = deploy_dir / "sync.sh"
    with open(shell_script, "w") as f:
        f.write(shell_content)
    
    # Make shell script executable
    os.chmod(shell_script, 0o755)
    print("✅ Shell script created")
    
    # Create README
    readme_content = """# SQL Anywhere Sync Tool

## Setup Instructions

1. **Configure Database Connection**
   - Edit `config.json` file
   - Update the DSN, username, and password for your SQL Anywhere database
   - Update the API base URL to point to your web API

2. **Run the Sync Tool**
   - **Windows**: Double-click `sync.bat` or run `SyncTool.exe` directly
   - **Linux/Mac**: Run `./sync.sh` or `./SyncTool` directly

## Configuration

Edit the `config.json` file to match your environment:

```json
{
  "database": {
    "dsn": "YOUR_DATABASE_DSN",
    "username": "YOUR_USERNAME", 
    "password": "YOUR_PASSWORD"
  },
  "api": {
    "base_url": "https://your-api-domain.com/api",
    "upload_endpoint": "/upload-users/",
    "timeout": 30
  }
}
```

## Requirements

- SQL Anywhere database with ODBC driver configured
- Internet connection to reach the web API
- Windows: No additional software required
- Linux/Mac: Ensure execute permissions are set

## Troubleshooting

- Check the log files created in the same directory for detailed error information
- Verify ODBC DSN is properly configured in your system
- Ensure the API endpoint is accessible from your network
- Check that the SQL Anywhere database table 'acc_users' exists and is accessible

## Support

For support, check the log files for detailed error messages.
"""
    
    with open(deploy_dir / "README.md", "w") as f:
        f.write(readme_content)
    print("✅ README file created")
    
    print(f"🎉 Deployment package created in '{deploy_dir}' directory")
    return True

def main():
    """Main build process"""
    print("=" * 60)
    print("    SQL Anywhere Sync Tool - Build Script")
    print("=" * 60)
    print()
    
    # Check Python version
    if sys.version_info < (3, 6):
        print("❌ Python 3.6 or higher is required")
        sys.exit(1)
    
    print(f"✅ Python {sys.version.split()[0]} detected")
    
    # Check and install dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Check and install PyInstaller
    if not check_pyinstaller():
        sys.exit(1)
    
    # Create build directory
    create_build_directory()
    
    # Build executable
    if not build_executable():
        sys.exit(1)
    
    # Create deployment package
    if not create_deployment_package():
        sys.exit(1)
    
    print()
    print("🎉 Build completed successfully!")
    print("📁 Check the 'SQLAnywhereSync' folder for the deployment package")
    print()
    print("Next steps:")
    print("1. Edit config.json with your database and API details")
    print("2. Test the executable on your system")
    print("3. Distribute the 'SQLAnywhereSync' folder to users")

if __name__ == "__main__":
    main()