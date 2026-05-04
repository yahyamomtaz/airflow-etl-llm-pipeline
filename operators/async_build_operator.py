"""
Async Build Trigger Operator
Triggers a remote build asynchronously and polls for completion
"""

from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
import time
import json


class AsyncBuildTriggerOperator(BaseOperator):
    """
    Triggers a build script on remote server asynchronously (in background).
    Does not wait for completion - use AsyncBuildSensor to wait.
    """
    
    def __init__(
        self,
        ssh_conn_id='ssh_default',
        build_script_path='/home/user/scripts/build_website.sh',
        status_file_path='/home/user/build_status.json',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.build_script_path = build_script_path
        self.status_file_path = status_file_path
    
    def execute(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        
        with ssh_hook.get_conn() as ssh_client:
            # Clear previous status file
            clear_cmd = f'rm -f {self.status_file_path}'
            ssh_client.exec_command(clear_cmd)
            
            # Trigger build in background (nohup)
            trigger_cmd = f'nohup bash {self.build_script_path} > /tmp/build_output.log 2>&1 &'
            
            self.log.info(f"🚀 Triggering async build: {trigger_cmd}")
            stdin, stdout, stderr = ssh_client.exec_command(trigger_cmd)
            
            # Don't wait - just confirm command was sent
            time.sleep(2)  # Brief wait to ensure script starts
            
            self.log.info("✅ Build triggered successfully (running in background)")
            
            # Push status file path to XCom for sensor
            context['ti'].xcom_push(key='status_file_path', value=self.status_file_path)
            
            return {'status': 'triggered', 'status_file': self.status_file_path}


class AsyncBuildSensor(BaseOperator):
    """
    Polls the remote status file until build completes.
    Raises error if build fails.
    """
    
    def __init__(
        self,
        ssh_conn_id='ssh_default',
        status_file_path='/home/user/build_status.json',
        poke_interval=10,  # Check every 10 seconds
        timeout=1800,  # 30 minutes max
        **kwargs
    ):
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.status_file_path = status_file_path
        self.poke_interval = poke_interval
        self.timeout = timeout
    
    def execute(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        
        start_time = time.time()
        
        self.log.info(f"⏳ Waiting for build to complete (polling {self.status_file_path})")
        
        while True:
            elapsed = time.time() - start_time
            
            if elapsed > self.timeout:
                raise TimeoutError(f"Build did not complete within {self.timeout} seconds")
            
            with ssh_hook.get_conn() as ssh_client:
                # Read status file
                cmd = f'cat {self.status_file_path} 2>/dev/null || echo "{{}}"'
                stdin, stdout, stderr = ssh_client.exec_command(cmd)
                status_json = stdout.read().decode('utf-8').strip()
                
                try:
                    status_data = json.loads(status_json) if status_json else {}
                except json.JSONDecodeError:
                    status_data = {}
                
                status = status_data.get('status', 'unknown')
                message = status_data.get('message', '')
                timestamp = status_data.get('timestamp', '')
                
                self.log.info(f"📊 Build status: {status} - {message} (elapsed: {int(elapsed)}s)")
                
                if status == 'success':
                    self.log.info("✅ Build completed successfully!")
                    return {'status': 'success', 'message': message, 'elapsed': elapsed}
                
                elif status == 'failed':
                    # Try to fetch build log for better error reporting
                    log_cmd = 'tail -50 /tmp/build.log 2>/dev/null || echo "Log not available"'
                    stdin_log, stdout_log, stderr_log = ssh_client.exec_command(log_cmd)
                    build_log = stdout_log.read().decode('utf-8').strip()
                    
                    error_msg = f"Build failed: {message}"
                    if build_log:
                        error_msg += f"\n\nLast 50 lines of build log:\n{build_log}"
                    
                    raise RuntimeError(error_msg)
                
                elif status == 'running':
                    self.log.info(f"⏳ Build still running... checking again in {self.poke_interval}s")
                
                else:
                    self.log.info(f"⏳ Waiting for build to start... checking again in {self.poke_interval}s")
            
            time.sleep(self.poke_interval)
