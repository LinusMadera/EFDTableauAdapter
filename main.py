import subprocess
import sys
import os
import requests
import time
import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from pathlib import Path
from datetime import datetime
import pandas as pd
#import winreg
import ctypes
from sql_queries import create_connection, insert_data_from_excel

# Translations dictionary
TRANSLATIONS = {
    'title': 'Programa de Importação de Dados para Liberdade Econômica',
    'system_status': 'Status do Sistema',
    'connection_info': 'Informações de Conexão',
    'start_sql': 'Iniciar SQL Server',
    'stop_sql': 'Parar SQL Server',
    'install_docker': 'Instalar Docker',
    'select_excel': 'Selecionar Arquivo Excel',
    'import_data': 'Importar Dados',
    'exit': 'Sair',
    'checking_docker': 'Verificando Docker...',
    'checking_sqlserver': 'Verificando SQL Server...',
    'docker_not_found': 'Docker não encontrado',
    'docker_running': 'Docker: Em execução',
    'docker_not_running': 'Docker: Não está em execução',
    'sqlserver_running': 'SQL Server: Em execução',
    'sqlserver_not_running': 'SQL Server: Parado',
    'starting_sqlserver': 'Iniciando SQL Server...',
    'sqlserver_started': 'SQL Server iniciado!',
    'stopping_sqlserver': 'Parando SQL Server...',
    'sqlserver_stopped': 'SQL Server parado com sucesso!',
    'error_starting': 'Erro ao iniciar SQL Server',
    'error_stopping': 'Erro ao parar SQL Server',
    'docker_required': 'Docker necessário',
    'install_docker_prompt': 'Docker não encontrado. Deseja instalar?',
    'admin_required': 'Permissão de administrador necessária',
    'admin_prompt': 'Esta aplicação requer privilégios de administrador. Deseja reiniciar como administrador?',
    'download_docker': 'Baixando Docker Desktop...',
    'installing_docker': 'Instalando Docker Desktop...',
    'docker_install_complete': 'Instalação do Docker Desktop concluída.',
    'docker_install_error': 'Erro na instalação do Docker: {}',
    'excel_import_success': 'Dados importados com sucesso!',
    'excel_import_error': 'Erro ao importar dados: {}',
    'error': 'Erro',
    'warning': 'Aviso'
}

# Docker Compose configuration
DOCKER_COMPOSE_CONTENT = """services:
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2022-latest
    container_name: sqlserver
    restart: unless-stopped
    environment:
      - ACCEPT_EULA=Y
      - MSSQL_SA_PASSWORD=TableauAdmin2023!
      - MSSQL_PID=Express
    ports:
      - "1433:1433"
    volumes:
      - sqlserver_data:/var/opt/mssql
    healthcheck:
      test: /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "TableauAdmin2023!" -Q "SELECT 1" || exit 1
      interval: 10s
      timeout: 3s
      retries: 10
      start_period: 10s

volumes:
  sqlserver_data:"""

class CombinedManagerGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title(TRANSLATIONS['title'])
        self.root.geometry("800x900")  # Increased window size
        self.root.configure(bg="#ffffff")
        
        # Configure style
        self.style = ttk.Style()
        self.style.configure("Header.TLabel", font=("Helvetica", 12, "bold"))
        self.style.configure("Status.TLabel", font=("Helvetica", 10))
        self.style.configure("Success.TLabel", foreground="green")
        self.style.configure("Error.TLabel", foreground="red")
        
        # Create main frame
        self.sql_frame = ttk.Frame(self.root)
        self.sql_frame.pack(expand=True, fill="both", padx=10, pady=10)
        
        # Initialize SQL manager interface
        self.setup_sql_manager()

    def setup_sql_manager(self):
        # System Status section
        ttk.Label(self.sql_frame, text=TRANSLATIONS['system_status'], style="Header.TLabel").pack(anchor="w", pady=(0, 5))
        
        # Status labels
        self.docker_status = ttk.Label(self.sql_frame, text="", style="Status.TLabel")
        self.docker_status.pack(anchor="w")
        
        self.sqlserver_status = ttk.Label(self.sql_frame, text="", style="Status.TLabel")
        self.sqlserver_status.pack(anchor="w")
        
        # Buttons frame
        button_frame = ttk.Frame(self.sql_frame)
        button_frame.pack(fill="x", pady=10)
        
        # SQL Server control buttons
        self.start_button = ttk.Button(button_frame, text="1. " + TRANSLATIONS['start_sql'], command=self.start_thread)
        self.start_button.pack(fill="x", pady=5)
        
        self.select_file_button = ttk.Button(button_frame, text="2. " + TRANSLATIONS['select_excel'], command=self.select_excel_file)
        self.select_file_button.pack(fill="x", pady=5)
        
        self.import_button = ttk.Button(button_frame, text="3. " + TRANSLATIONS['import_data'], command=self.import_excel_data)
        self.import_button.pack(fill="x", pady=5)
        self.import_button.config(state="disabled")
        
        self.stop_button = ttk.Button(button_frame, text="4. " + TRANSLATIONS['stop_sql'], command=self.stop_thread)
        self.stop_button.pack(fill="x", pady=5)
        
        # Exit button
        ttk.Button(button_frame, text="5. " + TRANSLATIONS['exit'], command=self.root.quit).pack(fill="x", pady=5)
        
        # Details section
        ttk.Label(self.sql_frame, text=TRANSLATIONS['connection_info'], style="Header.TLabel").pack(anchor="w", pady=(20, 5))
        self.details_text = tk.Text(self.sql_frame, height=15, width=60)
        self.details_text.pack(pady=5)
        
        # Initialize status check
        self.check_system_status()

    # SQL Manager Methods
    def is_docker_available(self):
        try:
            result = subprocess.run(["docker", "-v"], capture_output=True, text=True)
            return result.returncode == 0
        except FileNotFoundError:
            return False

    def is_docker_running(self):
        try:
            subprocess.run(["docker", "info"], capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    def check_system_status(self):
        if self.is_docker_available():
            if self.is_docker_running():
                self.docker_status.config(
                    text=TRANSLATIONS['docker_running'],
                    style="Success.TLabel"
                )
                self.add_detail("Docker detectado e em execução.")
            else:
                self.docker_status.config(
                    text=TRANSLATIONS['docker_not_running'],
                    style="Error.TLabel"
                )
                self.add_detail("Docker instalado mas não está em execução.")
                messagebox.showwarning(
                    TRANSLATIONS['warning'],
                    "Docker não está em execução. Por favor, inicie o Docker Desktop."
                )
        else:
            self.docker_status.config(
                text=TRANSLATIONS['docker_not_found'],
                style="Error.TLabel"
            )
            self.add_detail("Docker não encontrado no sistema.")
        self.check_sqlserver_status()

    def check_sqlserver_status(self):
        try:
            result = subprocess.run(
                ["docker", "container", "inspect", "sqlserver"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                self.sqlserver_status.config(
                    text=TRANSLATIONS['sqlserver_running'],
                    style="Success.TLabel"
                )
                self.stop_button.config(state="normal")
                self.start_button.config(state="disabled")
            else:
                self.sqlserver_status.config(
                    text=TRANSLATIONS['sqlserver_not_running'],
                    style="Error.TLabel"
                )
                self.stop_button.config(state="disabled")
                self.start_button.config(state="normal")
        except:
            self.sqlserver_status.config(
                text=TRANSLATIONS['sqlserver_not_running'],
                style="Error.TLabel"
            )
            self.stop_button.config(state="disabled")
            self.start_button.config(state="normal")

    def start_thread(self):
        if not self.is_docker_running():
            messagebox.showerror(
                TRANSLATIONS['error'],
                "Docker não está em execução. Por favor, inicie o Docker Desktop primeiro."
            )
            return

        def start_sqlserver():
            self.start_button.config(state="disabled")
            self.stop_button.config(state="disabled")

            try:
                self.add_detail(TRANSLATIONS['starting_sqlserver'])
                if not os.path.exists("docker-compose.yml"):
                    with open("docker-compose.yml", "w") as f:
                        f.write(DOCKER_COMPOSE_CONTENT)

                subprocess.run(["docker-compose", "up", "-d"], check=True)
                self.add_detail(TRANSLATIONS['sqlserver_started'])

            except Exception as e:
                self.add_detail(f"{TRANSLATIONS['error_starting']}: {e}")
                messagebox.showerror(TRANSLATIONS['error'], f"{TRANSLATIONS['error_starting']}: {e}")
            finally:
                self.check_sqlserver_status()

        thread = threading.Thread(target=start_sqlserver)
        thread.daemon = True
        thread.start()

    def stop_thread(self):
        def stop_sqlserver():
            self.start_button.config(state="disabled")
            self.stop_button.config(state="disabled")

            try:
                self.add_detail(TRANSLATIONS['stopping_sqlserver'])
                subprocess.run(["docker-compose", "down"], check=True)
                self.add_detail(TRANSLATIONS['sqlserver_stopped'])
            except Exception as e:
                self.add_detail(f"{TRANSLATIONS['error_stopping']}: {e}")
                messagebox.showerror(TRANSLATIONS['error'], f"{TRANSLATIONS['error_stopping']}: {e}")
            finally:
                self.check_sqlserver_status()

        thread = threading.Thread(target=stop_sqlserver)
        thread.daemon = True
        thread.start()

    def select_excel_file(self):
        self.excel_file_path = filedialog.askopenfilename(
            title="Selecionar arquivo Excel",
        )
        if self.excel_file_path:
            self.import_button.config(state="normal")
            self.add_detail(f"Arquivo selecionado: {self.excel_file_path}")

    def import_excel_data(self):
        if hasattr(self, 'excel_file_path'):
            try:
                connection = create_connection(
                    host="localhost",
                    user="sa",
                    password="TableauAdmin2023!"
                )
                
                if connection:
                    # Add error handling and data validation before insertion
                    try:
                        if insert_data_from_excel(self.excel_file_path, connection):
                            messagebox.showinfo("Sucesso", TRANSLATIONS['excel_import_success'])
                            self.add_detail("Dados importados com sucesso!")
                        else:
                            messagebox.showerror("Erro", TRANSLATIONS['excel_import_error'].format("Falha na importação"))
                    except Exception as e:
                        # Log the specific error for debugging
                        self.add_detail(f"Erro na importação: {str(e)}")
                        messagebox.showerror("Erro", f"Erro específico na importação: {str(e)}")
                    finally:
                        connection.close()
                else:
                    messagebox.showerror("Erro", "Não foi possível conectar ao banco de dados")
            except Exception as e:
                messagebox.showerror("Erro", TRANSLATIONS['excel_import_error'].format(str(e)))

    def add_detail(self, message):
        self.details_text.config(state="normal")
        self.details_text.insert(tk.END, f"{message}\n")
        self.details_text.see(tk.END)
        self.details_text.config(state="disabled")
        self.root.update()

    def run(self):
        self.root.mainloop()

def check_environment():

    if not check_docker():
        print("Docker check failed. Attempting to install Docker...")
        if not check_and_install_docker():
            print("Failed to install Docker. Please install Docker Desktop manually.")
            sys.exit(1)
            
def check_docker():

    try:
        subprocess.run(["docker", "--version"], check=True, capture_output=True)
        return True
    except (WindowsError, subprocess.CalledProcessError):
        return False

def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False

def run_as_admin():
    ctypes.windll.shell32.ShellExecuteW(
        None, 
        "runas", 
        sys.executable, 
        " ".join([sys.argv[0]] + sys.argv[1:]),
        None, 
        1  # SW_SHOWNORMAL
    )

def check_and_install_docker():

    def is_docker_installed():
        try:
            winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\Docker Desktop")
            
            subprocess.run(["docker", "--version"], check=True, capture_output=True)
            return True
        except (WindowsError, subprocess.CalledProcessError):
            return False

    def download_docker():
        docker_url = "https://desktop.docker.com/win/stable/Docker%20Desktop%20Installer.exe"
        installer_path = os.path.join(os.environ["TEMP"], "DockerDesktopInstaller.exe")
        
        print("Downloading Docker Desktop installer...")
        response = requests.get(docker_url, stream=True)
        with open(installer_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return installer_path

    def install_docker(installer_path):
        print("Installing Docker Desktop...")
        try:
            subprocess.run([installer_path, "install", "--quiet"], check=True)
            print("Docker installation completed.")
            
            docker_path = r"C:\Program Files\Docker\Docker\Docker Desktop.exe"
            if os.path.exists(docker_path):
                print("Starting Docker Desktop...")
                subprocess.Popen([docker_path])
                time.sleep(45)
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error installing Docker: {e}")
            return False
        finally:
            try:
                os.remove(installer_path)
            except OSError:
                pass

    if sys.platform != "win32":
        print("This script is intended for Windows systems only.")
        return False

    if is_docker_installed():
        print("Docker is already installed.")
        return True
    else:
        print("Docker is not installed.")
        
        if sys.getwindowsversion().major < 10:
            print("Docker Desktop requires Windows 10 or later.")
            return False
            
        try:
            installer_path = download_docker()
            if install_docker(installer_path):
                print("Waiting for Docker services to start...")
                time.sleep(30)
                
                if is_docker_installed():
                    print("Docker installation verified successfully.")
                    return True
                else:
                    print("Docker installation completed but verification failed.")
                    print("Please restart your computer and try running Docker Desktop manually.")
                    return False
        except Exception as e:
            print(f"An error occurred during installation: {e}")
            return False

if __name__ == "__main__":
    if not is_admin() and sys.platform == "win32":
        print("Requesting administrator privileges...")
        run_as_admin()

    check_environment()
    app = CombinedManagerGUI()
    app.run()