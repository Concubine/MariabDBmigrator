"""Builder script for creating a standalone executable."""
import os
import platform
import shutil
import subprocess
import sys
import venv
from pathlib import Path

def create_venv(venv_path: Path) -> None:
    """Create a virtual environment."""
    print(f"Creating virtual environment in {venv_path}")
    venv.create(venv_path, with_pip=True)

def get_python_path(venv_path: Path) -> str:
    """Get the Python executable path from the virtual environment."""
    if platform.system() == "Windows":
        return str(venv_path / "Scripts" / "python.exe")
    return str(venv_path / "bin" / "python")

def get_pip_path(venv_path: Path) -> str:
    """Get the pip executable path from the virtual environment."""
    if platform.system() == "Windows":
        return str(venv_path / "Scripts" / "pip.exe")
    return str(venv_path / "bin" / "pip")

def upgrade_pip(python_path: str) -> None:
    """Upgrade pip to the latest version."""
    print("Upgrading pip...")
    subprocess.run([python_path, "-m", "pip", "install", "--upgrade", "pip"], check=True)

def install_requirements(pip_path: str, requirements_path: Path) -> None:
    """Install requirements from requirements.txt."""
    print("Installing requirements...")
    subprocess.run([pip_path, "install", "-r", str(requirements_path)], check=True)

def install_pyinstaller(pip_path: str) -> None:
    """Install PyInstaller."""
    print("Installing PyInstaller...")
    subprocess.run([pip_path, "install", "pyinstaller"], check=True)

def build_executable(python_path: str, main_script: Path) -> None:
    """Build the executable using PyInstaller."""
    print("Building executable...")
    # Set the working directory to the root directory
    root_dir = Path(__file__).parent
    # Use a spec file approach for more reliable imports
    cmd = [
        python_path,
        "-m",
        "PyInstaller",
        "--onefile",
        "--name",
        "mariadbexport",
        # Instead of adding as data, include as modules
        "--add-data",
        f"config{os.pathsep}config",
        # Add the root directory to Python path to help with imports
        "--paths",
        str(root_dir),
        # Key imports
        "--hidden-import",
        "src",
        "--hidden-import",
        "src.core.config",
        "--hidden-import",
        "src.core.exceptions",
        "--hidden-import",
        "src.domain.models",
        "--hidden-import",
        "src.infrastructure.mariadb",
        "--hidden-import",
        "src.infrastructure.storage",
        "--hidden-import",
        "src.services.export",
        "--hidden-import",
        "src.services.import_",
        "--hidden-import",
        "yaml",
        "--hidden-import",
        "mysql.connector",
        "--hidden-import",
        "sqlparse",
        str(main_script)
    ]
    subprocess.run(cmd, check=True)

def copy_config(config_path: Path, dist_path: Path) -> None:
    """Copy the config file to the dist folder."""
    print("Copying config file...")
    # Create config directory in dist folder
    config_dir = dist_path / "config"
    config_dir.mkdir(exist_ok=True)
    # Copy config file to config/config.yaml
    shutil.copy2(config_path, config_dir / "config.yaml")

def main() -> None:
    """Main build process."""
    # Define paths
    root_dir = Path(__file__).parent
    venv_path = root_dir / "venv"
    requirements_path = root_dir / "requirements.txt"
    main_script = root_dir / "src" / "main.py"
    config_path = root_dir / "config" / "config.yaml"
    dist_path = root_dir / "dist"

    try:
        # Create virtual environment
        create_venv(venv_path)
        
        # Get paths to Python and pip executables
        python_path = get_python_path(venv_path)
        pip_path = get_pip_path(venv_path)
        
        # Upgrade pip
        upgrade_pip(python_path)
        
        # Install requirements
        install_requirements(pip_path, requirements_path)
        
        # Install PyInstaller
        install_pyinstaller(pip_path)
        
        # Build executable
        build_executable(python_path, main_script)
        
        # Copy config file
        copy_config(config_path, dist_path)
        
        print("\nBuild completed successfully!")
        print(f"Executable and config file are in: {dist_path}")
        
    except subprocess.CalledProcessError as e:
        print(f"Error during build process: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 