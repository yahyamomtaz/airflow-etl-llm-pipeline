from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook


class NextJSBuilderOperator(BaseOperator):
    """
    Connects to a remote server via SSH and builds Next.js site.
    """
    template_fields = ('remote_project_dir',)

    def __init__(
        self,
        task_id,
        remote_project_dir='website/main',
        ssh_conn_id='ssh_default',
        **kwargs
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.remote_project_dir = remote_project_dir
        self.ssh_conn_id = ssh_conn_id

    def execute(self, context):
        # Connect via SSH and run build commands
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        
        with ssh_hook.get_conn() as ssh_client:
            self._run_remote_build(ssh_client)

    def _run_remote_build(self, ssh_client):
        """Run build commands on the remote server."""
        commands = [
            f"cd {self.remote_project_dir}",
            "npm run build",
            "pm2 restart website"  # configure: replace with your pm2 app name
        ]
        
        # Combine commands to run in sequence
        full_command = " && ".join(commands)
        
        self.log.info(f"🚀 Running remote build: {full_command}")
        
        stdin, stdout, stderr = ssh_client.exec_command(full_command)
        
        # Wait for command to complete and get exit status
        exit_status = stdout.channel.recv_exit_status()
        
        # Log output
        stdout_text = stdout.read().decode('utf-8')
        stderr_text = stderr.read().decode('utf-8')
        
        if stdout_text:
            self.log.info(f"📤 stdout:\n{stdout_text}")
        if stderr_text:
            self.log.warning(f"⚠️ stderr:\n{stderr_text}")
        
        if exit_status != 0:
            raise RuntimeError(f"Remote build failed with exit code {exit_status}")
        
        self.log.info("✅ Remote build completed successfully")
