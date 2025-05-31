import logging
import boto3
import re
import time
from datetime import datetime, timedelta
from utils import calculate_monthly_cost
from botocore.exceptions import ClientError

logger = logging.getLogger()

class OverprovisionedVolumeDetector:
    """
    과대 프로비저닝된 EBS 볼륨을 감지하는 클래스
    """
    
    def __init__(self, region, ec2_client, cloudwatch_client, criteria):
        """
        :param region: AWS 리전
        :param ec2_client: EC2 클라이언트
        :param cloudwatch_client: CloudWatch 클라이언트
        :param criteria: 과대 프로비저닝 감지 기준
        """
        self.region = region
        self.ec2_client = ec2_client
        self.cloudwatch_client = cloudwatch_client
        self.criteria = criteria
        # SSM 클라이언트 초기화 (EC2 내부 파일시스템 정보 수집용)
        self.ssm_client = boto3.client('ssm', region_name=region)
        # 인스턴스 SSM 상태 캐시 (성능 향상을 위해)
        self.instance_ssm_status_cache = {}
    
    def check_instance_ssm_status(self, instance_id):
        """
        인스턴스가 SSM 명령을 실행할 수 있는 상태인지 확인
        
        :param instance_id: EC2 인스턴스 ID
        :return: (가능 여부, 상태 메시지)
        """
        # 캐시된 결과가 있으면 반환
        if instance_id in self.instance_ssm_status_cache:
            return self.instance_ssm_status_cache[instance_id]
            
        try:
            # 인스턴스 상태 확인
            ec2_response = self.ec2_client.describe_instances(InstanceIds=[instance_id])
            if not ec2_response['Reservations'] or not ec2_response['Reservations'][0]['Instances']:
                result = (False, f"인스턴스 {instance_id}를 찾을 수 없습니다.")
                self.instance_ssm_status_cache[instance_id] = result
                return result
                
            instance = ec2_response['Reservations'][0]['Instances'][0]
            state = instance.get('State', {}).get('Name', '')
            
            if state != 'running':
                result = (False, f"인스턴스 {instance_id}가 실행 중이 아닙니다(현재 상태: {state}).")
                self.instance_ssm_status_cache[instance_id] = result
                return result
            
            # SSM에서 관리되는 인스턴스인지 확인
            try:
                ssm_response = self.ssm_client.describe_instance_information(
                    Filters=[{'Key': 'InstanceIds', 'Values': [instance_id]}]
                )
                
                if not ssm_response['InstanceInformationList']:
                    result = (False, f"인스턴스 {instance_id}가 SSM에 등록되지 않았습니다.")
                    self.instance_ssm_status_cache[instance_id] = result
                    return result
                
                ping_status = ssm_response['InstanceInformationList'][0].get('PingStatus', '')
                if ping_status != 'Online':
                    result = (False, f"인스턴스 {instance_id}의 SSM Agent가 온라인 상태가 아닙니다(현재 상태: {ping_status}).")
                    self.instance_ssm_status_cache[instance_id] = result
                    return result
                
                result = (True, "인스턴스가 SSM 명령을 실행할 수 있는 상태입니다.")
                self.instance_ssm_status_cache[instance_id] = result
                return result
            except Exception as ssm_error:
                # SSM 서비스 오류(권한 부족 등)가 발생한 경우
                logger.warning(f"SSM 서비스 오류: {str(ssm_error)}")
                result = (False, f"SSM 서비스 오류: {str(ssm_error)}")
                self.instance_ssm_status_cache[instance_id] = result
                return result
            
        except Exception as e:
            logger.warning(f"인스턴스 {instance_id} 상태 확인 중 오류: {str(e)}")
            result = (False, f"인스턴스 상태 확인 오류: {str(e)}")
            self.instance_ssm_status_cache[instance_id] = result
            return result
    
    def get_disk_usage_metrics(self, instance_id, device_name, start_time, end_time):
        """
        CloudWatch 에이전트를 통해 수집된 디스크 사용률 지표를 가져옴
        
        :param instance_id: EC2 인스턴스 ID
        :param device_name: 디바이스 이름
        :param start_time: 측정 시작 시간
        :param end_time: 측정 종료 시간
        :return: 디스크 사용률 지표
        """
        # 먼저 CloudWatch 메트릭 확인
        try:
            # 인스턴스에 연결된 모든 볼륨의 CloudWatch 메트릭 확인
            metrics = self.cloudwatch_client.list_metrics(
                Namespace='CWAgent',
                MetricName='disk_used_percent',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}]
            )
            
            # CloudWatch에 메트릭이 있으면 메트릭 사용
            if metrics.get('Metrics'):
                paths = set()
                for metric in metrics['Metrics']:
                    for dim in metric['Dimensions']:
                        if dim['Name'] == 'path':
                            paths.add(dim['Value'])
                
                # 경로 정보 로깅
                if paths:
                    logger.info(f"인스턴스 {instance_id}에서 발견된 디스크 경로: {paths}")
                else:
                    logger.warning(f"인스턴스 {instance_id}에서 디스크 경로를 찾을 수 없습니다. 모든 차원 정보: {[metric['Dimensions'] for metric in metrics['Metrics']]}")
                
                # 루트 디바이스인 경우 '/' 경로 사용 시도
                device_short_name = device_name.split('/')[-1]
                if device_short_name in ['xvda', 'sda', 'nvme0n1'] or device_short_name.startswith('xvda') or device_short_name.startswith('sda'):
                    if '/' in paths:
                        logger.info(f"루트 디바이스 {device_name}에 대해 경로 \'/\'를 사용합니다.")
                        response = self.cloudwatch_client.get_metric_statistics(
                            Namespace='CWAgent',
                            MetricName='disk_used_percent',
                            Dimensions=[
                                {'Name': 'InstanceId', 'Value': instance_id},
                                {'Name': 'path', 'Value': '/'}
                            ],
                            StartTime=start_time,
                            EndTime=end_time,
                            Period=86400,
                            Statistics=['Average']
                        )
                        
                        if response['Datapoints']:
                            return response['Datapoints']
                
                # 가장 적합한 경로 찾기 시도
                fs_path = self.estimate_filesystem_path(device_name, paths)
                
                if fs_path:
                    logger.info(f"디바이스 {device_name}에 대해 추정된 경로: {fs_path}")
                    
                    response = self.cloudwatch_client.get_metric_statistics(
                        Namespace='CWAgent',
                        MetricName='disk_used_percent',
                        Dimensions=[
                            {'Name': 'InstanceId', 'Value': instance_id},
                            {'Name': 'path', 'Value': fs_path}
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=86400,  # 1일 단위
                        Statistics=['Average']
                    )
                    
                    if response['Datapoints']:
                        return response['Datapoints']
                    
            # 기타 모든 방법을 시도 후 실패하면 직접 마운트 정보 조회
            logger.info(f"CloudWatch에서 인스턴스 {instance_id}의 디스크 사용률 메트릭을 찾을 수 없습니다. 대체 방법 사용...")
            
            # 디바이스가 루트 볼륨인 경우 바로 SSM 통해 루트 볼륨 확인
            device_short_name = device_name.split('/')[-1]
            if device_short_name in ['xvda', 'sda', 'nvme0n1'] or device_short_name.startswith('xvda') or device_short_name.startswith('sda'):
                logger.info(f"루트 디바이스 {device_name} 감지됨. SSM을 통해 루트 파티션 사용률을 확인합니다.")
                datapoints = self.get_root_disk_usage_via_ssm(instance_id)
                if datapoints:
                    return datapoints
            
            # 일반적인 SSM 경로 사용
            ssm_status, message = self.check_instance_ssm_status(instance_id)
            if ssm_status:
                # SSM을 통해 디스크 사용률 조회 시도
                ssm_disk_usage = self.get_disk_usage_via_ssm(instance_id, device_name)
                if ssm_disk_usage:
                    return ssm_disk_usage
                else: # SSM 조회도 실패
                    logger.warning(f"SSM을 통해서도 인스턴스 {instance_id}, 디바이스 {device_name}의 디스크 사용률을 가져올 수 없습니다. 사용률 데이터 없음으로 처리합니다.")
                    return None # 수정: 추정치 대신 None 반환
            else:
                logger.warning(f"SSM을 사용할 수 없습니다: {message}. 사용률 데이터 없음으로 처리합니다.")
                return None # 수정: 추정치 대신 None 반환
            
        except Exception as e:
            logger.error(f"CloudWatch 메트릭 조회 중 오류 발생: {str(e)}", exc_info=True)
            # 오류 발생 시 None 반환
            logger.warning(f"CloudWatch 메트릭 조회 오류로 인해 인스턴스 {instance_id}, 디바이스 {device_name}의 디스크 사용률을 가져올 수 없습니다. 사용률 데이터 없음으로 처리합니다.")
            return None # 수정: 추정치 대신 None 반환
    
    def estimate_filesystem_path(self, device_name, available_paths):
        """
        디바이스 이름과 사용 가능한 경로 목록을 기반으로 가장 적합한 경로 추정
        
        :param device_name: 디바이스 이름 
        :param available_paths: 사용 가능한 경로 목록
        :return: 추정된 경로 또는 None
        """
        # 사용 가능한 경로가 없으면 None 반환
        if not available_paths:
            return None
        
        # 디바이스 이름 단순화 (예: /dev/xvdf -> xvdf)
        simple_device_name = device_name.split('/')[-1]
        
        # 일반적인 마운트 경로 패턴
        # 예: /data, /mnt/data, /vol, /var/lib/mysql 등
        # 루트 디바이스 (예: /dev/xvda, /dev/sda) -> 일반적으로 '/' 경로 사용
        if simple_device_name in ['xvda', 'sda', 'nvme0n1'] or simple_device_name.startswith('xvda') or simple_device_name.startswith('sda'):
            if '/' in available_paths:
                return '/'
        
        # 일반적인 데이터 볼륨 마운트 경로 확인
        # /data, /mnt/data, /var/log 등 다양한 경로가 있을 수 있습니다.
        # 이 부분은 특정 환경에 맞게 커스터마이징이 필요할 수 있습니다.
        # 여기서는 가장 일반적인 '/' 경로를 기본으로 하고, 다른 경로가 있으면 반환하는 방식으로 단순화합니다.
        # 더 정교한 로직을 원한다면, 인스턴스 내에서 `df -h`와 같은 명령을 실행하여 마운트 정보를 직접 가져오는 것이 좋습니다.
        for path in available_paths:
            # 루트 경로는 이미 처리했으므로, 루트가 아닌 경로 중 하나를 선택
            if path != '/':
                # 디바이스 이름이 경로에 포함되어 있는지 확인 (예: /mnt/xvdf)
                if simple_device_name in path:
                    return path
        
        # 위에서 적합한 경로를 찾지 못한 경우, 사용 가능한 경로 중 첫 번째 것을 반환 (루트 제외)
        non_root_paths = [p for p in available_paths if p != '/']
        if non_root_paths:
            return non_root_paths[0]
        
        # 모든 경로가 루트 뿐이라면 루트 반환
        if '/' in available_paths:
            return '/'
            
        return None # 적합한 경로를 찾지 못함
        
    def get_disk_usage_via_ssm(self, instance_id, device_name):
        """
        SSM Run Command를 사용하여 디스크 사용률을 가져옵니다.
        Linux: df -h, Windows: Get-PSDrive
        
        :param instance_id: EC2 인스턴스 ID
        :param device_name: 디바이스 이름 (예: /dev/xvdf)
        :return: 디스크 사용률 데이터포인트 리스트 또는 None
        """
        try:
            # 인스턴스 플랫폼 확인 (Linux 또는 Windows)
            response = self.ec2_client.describe_instances(InstanceIds=[instance_id])
            platform = response['Reservations'][0]['Instances'][0].get('PlatformDetails', 'Linux/UNIX').lower()
            
            if 'windows' in platform:
                # Windows: PowerShell 명령 실행
                command = f"Get-PSDrive | Where-Object {{ $_.Provider.Name -eq 'FileSystem' }} | Select-Object Name, @{{Name=\"UsedPercent\";Expression={{($_.Used / ($_.Used + $_.Free)) * 100}}}} | ConvertTo-Json"
                document_name = 'AWS-RunPowerShellScript'
            else:
                # Linux: df 명령 실행
                command = f"df -h | grep '{device_name}' | awk '{{print $5}}' | sed 's/%//'"
                # 만약 device_name이 파티션 번호를 포함하지 않는 경우 (예: /dev/xvdf 대신 /dev/xvdf1을 찾아야 함)
                # 좀 더 일반적인 명령: df -h | awk -v dev="{device_name}" '$1 ~ dev {{print $5}}' | sed 's/%//'
                # 이 명령은 device_name으로 시작하는 모든 파티션을 찾습니다 (예: /dev/xvdf1, /dev/xvdf2 등).
                # 여기서는 정확한 디바이스 이름을 사용한다고 가정합니다.
                document_name = 'AWS-RunShellScript'
            
            logger.info(f"SSM Run Command 실행: 인스턴스 {instance_id}, 명령: {command}")
            
            response = self.ssm_client.send_command(
                InstanceIds=[instance_id],
                DocumentName=document_name,
                Parameters={'commands': [command]},
                TimeoutSeconds=300 # 타임아웃 시간 (초)
            )
            
            command_id = response['Command']['CommandId']
            
            # 명령 실행 완료까지 대기 (최대 30초)
            for _ in range(6):
                time.sleep(5)
                output = self.ssm_client.get_command_invocation(
                    CommandId=command_id,
                    InstanceId=instance_id
                )
                if output['Status'] in ['Success', 'Failed', 'TimedOut', 'Cancelled']:
                    break
            else:
                logger.warning(f"SSM 명령 {command_id} 실행 시간이 초과되었습니다.")
                return None
            
            if output['Status'] == 'Success':
                command_output = output['StandardOutputContent'].strip()
                logger.info(f"SSM 명령 실행 결과: {command_output}")
                
                if 'windows' in platform:
                    # PowerShell 결과 파싱 (JSON 형식)
                    try:
                        # 결과는 JSON 배열일 수 있음, 예: [{"Name":"C","UsedPercent":75.2}, ...]
                        drive_data_list = json.loads(command_output)
                        # device_name과 가장 유사한 드라이브 찾기 (예: D: -> D)
                        # Windows 디바이스 이름은 보통 'C:', 'D:' 형식이지만, EBS 볼륨은 다른 방식으로 매핑될 수 있음.
                        # 여기서는 device_name (예: /dev/sdf -> f)을 기반으로 드라이브 문자를 추정합니다.
                        # 이 로직은 불완전하며, 더 정확한 매핑 방법이 필요할 수 있습니다.
                        estimated_drive_letter = device_name[-1].upper() # 매우 단순한 추정
                        
                        for drive_data in drive_data_list:
                            if drive_data['Name'] == estimated_drive_letter:
                                used_percent = float(drive_data['UsedPercent'])
                                return [{'Timestamp': datetime.now(), 'Average': used_percent, 'Unit': 'Percent'}]
                        logger.warning(f"Windows 드라이브 {estimated_drive_letter}에 대한 사용률 정보를 찾을 수 없습니다.")
                        return None
                    except json.JSONDecodeError:
                        logger.error(f"SSM PowerShell 결과 JSON 파싱 오류: {command_output}")
                        return None
                else:
                    # Linux 결과 파싱 (숫자 값)
                    if command_output.isdigit():
                        used_percent = float(command_output)
                        return [{'Timestamp': datetime.now(), 'Average': used_percent, 'Unit': 'Percent'}]
                    else:
                        logger.warning(f"SSM Shell 결과가 숫자가 아닙니다: {command_output}")
                        return None
            else:
                logger.error(f"SSM 명령 {command_id} 실행 실패: {output['Status']} - {output['StandardErrorContent']}")
                return None
                
        except Exception as e:
            logger.error(f"SSM을 통한 디스크 사용률 조회 중 오류 발생: {str(e)}", exc_info=True)
            return None
            
    def get_estimated_disk_usage(self, instance_id, device_name):
        """
        다른 방법으로 디스크 사용률을 가져올 수 없을 때, None을 반환합니다.
        이 함수는 모든 다른 방법이 실패했을 때 최후의 수단으로 사용됩니다.
        호출된다는 것은 데이터 수집에 문제가 있음을 의미합니다.
        
        :param instance_id: EC2 인스턴스 ID
        :param device_name: 디바이스 이름
        :return: None
        """
        logger.warning(f"get_estimated_disk_usage 호출됨: 인스턴스 {instance_id}, 디바이스 {device_name}의 디스크 사용률을 정확히 알 수 없습니다. 이는 데이터 수집 경로에 문제가 있음을 나타냅니다. 사용률 데이터 없음(None)으로 처리합니다.")
        # 실제로는 이 값을 0으로 설정하면 항상 과대 프로비저닝으로 판단될 수 있으므로 주의 필요
        # 또는 특정 상황에서는 분석에서 제외하거나, 사용자에게 알림을 보내는 등의 처리가 필요할 수 있습니다.
        return None # 수정: 0% 대신 None 반환

    # ---- HELPER FUNCTIONS ----
    
    def get_filesystem_path_safe(self, instance_id, device_name):
        """
        주어진 디바이스 이름에 해당하는 파일 시스템 경로를 안전하게 가져옵니다.
        SSM을 사용하여 인스턴스 내부에서 `lsblk -f -n -o MOUNTPOINT /dev/sdX` 와 유사한 명령을 실행합니다.
        
        :param instance_id: EC2 인스턴스 ID
        :param device_name: 디바이스 이름 (예: /dev/xvdf)
        :return: 파일 시스템 경로 또는 None
        """
        try:
            # 인스턴스 플랫폼 확인
            response = self.ec2_client.describe_instances(InstanceIds=[instance_id])
            platform = response['Reservations'][0]['Instances'][0].get('PlatformDetails', 'Linux/UNIX').lower()
            
            if 'windows' in platform:
                logger.warning(f"Windows 인스턴스 {instance_id}에 대한 파일 시스템 경로 조회는 현재 지원되지 않습니다.")
                return None # Windows는 다른 방법으로 처리해야 함
            
            # Linux: lsblk 명령 사용
            # 디바이스 이름에서 파티션 번호 제거 (예: /dev/xvdf1 -> /dev/xvdf)
            base_device_name = re.sub(r'[0-9]+$', '', device_name)
            command = f"lsblk -f -n -o MOUNTPOINT {base_device_name} | head -n 1"
            
            logger.info(f"SSM Run Command 실행 (파일 시스템 경로 조회): 인스턴스 {instance_id}, 명령: {command}")
            
            response = self.ssm_client.send_command(
                InstanceIds=[instance_id],
                DocumentName='AWS-RunShellScript',
                Parameters={'commands': [command]},
                TimeoutSeconds=60
            )
            command_id = response['Command']['CommandId']
            
            # 명령 완료 대기 (최대 30초)
            for _ in range(6):
                time.sleep(5)
                output = self.ssm_client.get_command_invocation(
                    CommandId=command_id,
                    InstanceId=instance_id
                )
                if output['Status'] in ['Success', 'Failed', 'TimedOut', 'Cancelled']:
                    break
            else:
                logger.warning(f"SSM 명령(파일 시스템 경로 조회) {command_id} 실행 시간이 초과되었습니다.")
                return None
            
            if output['Status'] == 'Success':
                mount_point = output['StandardOutputContent'].strip()
                if mount_point and mount_point != "":
                    logger.info(f"디바이스 {device_name}의 마운트 지점: {mount_point}")
                    return mount_point
                else:
                    logger.warning(f"디바이스 {device_name}에 대한 마운트 지점을 찾을 수 없습니다 (출력: '{mount_point}'). 루트('/')로 가정합니다.")
                    # 마운트 지점을 찾을 수 없는 경우, 루트 디바이스의 일부일 가능성이 높음
                    # 또는 아직 마운트되지 않았을 수 있음. 이 경우 루트로 가정하는 것은 위험할 수 있음.
                    # 더 정확한 처리를 위해서는 인스턴스 부팅 시점 등을 고려해야 함.
                    return '/' # 안전하게는 None을 반환하거나, 좀 더 확실한 기본값을 사용해야 함
            else:
                logger.error(f"SSM 명령(파일 시스템 경로 조회) {command_id} 실행 실패: {output['Status']} - {output['StandardErrorContent']}")
                return None
                
        except Exception as e:
            logger.error(f"SSM을 통한 파일 시스템 경로 조회 중 오류 발생: {str(e)}", exc_info=True)
            return None

    def get_filesystem_info(self, instance_id, device_name):
        """
        SSM Run Command를 사용하여 특정 디바이스의 파일 시스템 정보를 가져옵니다.
        Linux: `df -T /dev/sdX` 또는 `lsblk -f -n -o FSTYPE,MOUNTPOINT /dev/sdX`
        Windows: `Get-Volume -FilePath (Get-Partition -DiskNumber X -PartitionNumber Y).AccessPaths[0] | Select-Object FileSystem` (복잡함)
        
        주로 파일 시스템 유형(ext4, xfs 등)과 마운트 지점을 확인하는 데 사용됩니다.
        
        :param instance_id: EC2 인스턴스 ID
        :param device_name: 디바이스 이름 (예: /dev/xvdf)
        :return: { 'fstype': 'ext4', 'mountpoint': '/data' } 형식의 딕셔너리 또는 None
        """
        try:
            # 인스턴스 플랫폼 확인
            response = self.ec2_client.describe_instances(InstanceIds=[instance_id])
            platform = response['Reservations'][0]['Instances'][0].get('PlatformDetails', 'Linux/UNIX').lower()

            if 'windows' in platform:
                # Windows의 경우 PowerShell을 사용하여 볼륨 정보를 가져올 수 있습니다.
                # 예: Get-Disk | Get-Partition | Get-Volume (복잡하여 여기서는 생략)
                logger.warning(f"Windows 인스턴스 {instance_id}의 파일 시스템 정보 조회는 현재 구현되지 않았습니다.")
                return None
            
            # Linux: lsblk 명령 사용 (더 안정적이고 다양한 정보 제공)
            # -f: 파일 시스템 정보 출력
            # -n: 헤더 없이 출력
            # -o FSTYPE,MOUNTPOINT: 원하는 컬럼만 선택
            # {device_name}에는 파티션 번호가 포함될 수 있음 (예: /dev/xvdf1)
            command = f"lsblk -f -n -o FSTYPE,MOUNTPOINT {device_name} | head -n 1"
            
            logger.info(f"SSM Run Command 실행 (파일 시스템 정보 조회): 인스턴스 {instance_id}, 명령: {command}")
            
            response = self.ssm_client.send_command(
                InstanceIds=[instance_id],
                DocumentName='AWS-RunShellScript',
                Parameters={'commands': [command]},
                TimeoutSeconds=60
            )
            command_id = response['Command']['CommandId']
            
            # 명령 완료 대기 (최대 30초)
            for _ in range(6):
                time.sleep(5) # 5초 대기
                output = self.ssm_client.get_command_invocation(
                    CommandId=command_id,
                    InstanceId=instance_id
                )
                # 상태가 최종 상태 중 하나이면 루프 종료
                if output['Status'] in ['Success', 'Failed', 'TimedOut', 'Cancelled']:
                    break
            else:
                # 타임아웃 발생
                logger.warning(f"SSM 명령(파일 시스템 정보 조회) {command_id} 실행 시간이 초과되었습니다.")
                return None # 또는 오류 상태 반환
            
            if output['Status'] == 'Success':
                # 출력 형식 예시: "ext4 /data" 또는 "xfs" (마운트 안된 경우)
                content = output['StandardOutputContent'].strip()
                if not content:
                    logger.warning(f"파일 시스템 정보를 찾을 수 없습니다 (디바이스: {device_name}, 인스턴스: {instance_id}). 출력이 비어있습니다.")
                    return None
                    
                parts = content.split()
                fstype = parts[0] if len(parts) > 0 else None
                mountpoint = parts[1] if len(parts) > 1 else None
                
                # 마운트되지 않은 볼륨의 경우 (예: 'swap' 또는 파일 시스템 타입만 출력됨)
                if mountpoint is None and fstype and not fstype.startswith('/'):
                    logger.info(f"디바이스 {device_name}은 마운트되지 않았거나 스왑 파티션일 수 있습니다 (FSTYPE: {fstype}).")
                    return {'fstype': fstype, 'mountpoint': None}
                elif fstype is None and mountpoint is None:
                    logger.warning(f"디바이스 {device_name}의 FSTYPE과 MOUNTPOINT를 파싱할 수 없습니다 (출력: '{content}').")
                    return None
                
                logger.info(f"디바이스 {device_name} 정보: FSTYPE={fstype}, MOUNTPOINT={mountpoint}")
                return {'fstype': fstype, 'mountpoint': mountpoint}
            else:
                logger.error(f"SSM 명령(파일 시스템 정보 조회) {command_id} 실행 실패: {output['Status']} - {output['StandardErrorContent']}")
                return None
                
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'InvalidInstanceId':
                logger.error(f"잘못된 인스턴스 ID: {instance_id}")
            else:
                logger.error(f"SSM ClientError: {str(ce)}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"SSM을 통한 파일 시스템 정보 조회 중 예상치 못한 오류 발생: {str(e)}", exc_info=True)
            return None
            
    def get_default_filesystem_path(self, device_name):
        """
        디바이스 이름에 따라 기본 파일 시스템 경로를 추정합니다.
        이 함수는 CloudWatch 에이전트에서 올바른 'path' 차원을 찾지 못했을 때 사용될 수 있습니다.
        정확성은 보장되지 않으며, 실제 환경에 따라 조정이 필요합니다.
        
        :param device_name: 디바이스 이름 (예: /dev/xvdf)
        :return: 추정된 파일 시스템 경로 (예: /data) 또는 '/'
        """
        # 루트 디바이스 이름 패턴 (Nitro 인스턴스 포함)
        root_device_patterns = [r'/dev/xvda', r'/dev/sda', r'/dev/nvme0n1']
        # 루트 파티션 이름 패턴 (예: /dev/xvda1, /dev/sda1, /dev/nvme0n1p1)
        root_partition_patterns = [r'/dev/xvda[0-9]+', r'/dev/sda[0-9]+', r'/dev/nvme0n1p[0-9]+']

        # 디바이스 이름이 루트 디바이스 또는 루트 파티션 패턴과 일치하는지 확인
        for pattern in root_device_patterns + root_partition_patterns:
            if re.fullmatch(pattern, device_name):
                logger.info(f"디바이스 {device_name}은 루트 디바이스 또는 파티션으로 추정됩니다. 경로: '/'")
                return '/' # 루트 디바이스는 일반적으로 '/'에 마운트됨

        # 일반적인 데이터 볼륨 마운트 포인트 추정
        # 예: /dev/xvdf -> /data, /dev/sdb -> /mnt/vol1 등
        # 이 부분은 매우 일반적인 예시이며, 실제 환경에 따라 다를 수 있습니다.
        if 'xvdf' in device_name or 'sdf' in device_name:
            return '/data' # /dev/xvdf, /dev/sdf 등은 종종 /data에 마운트됨
        elif 'xvdg' in device_name or 'sdg' in device_name:
            return '/data2'
        elif 'nvme1n1' in device_name:
             return '/data' # 추가 NVMe 드라이브
        
        # 위에 해당하지 않는 경우, 기본적으로 루트 경로('/') 또는 더 구체적인 기본값 반환
        # 이는 해당 볼륨이 루트 파일 시스템의 일부로 사용될 가능성을 의미할 수 있지만, 항상 그렇지는 않음.
        logger.warning(f"디바이스 {device_name}에 대한 기본 파일 시스템 경로를 결정할 수 없습니다. 루트('/')로 가정합니다.")
        return '/'

    def get_root_disk_usage_via_ssm(self, instance_id):
        """
        SSM Run Command를 사용하여 루트(/) 파티션의 디스크 사용률을 가져옵니다.
        
        :param instance_id: EC2 인스턴스 ID
        :return: 디스크 사용률 데이터포인트 리스트 또는 None
        """
        try:
            # 인스턴스 플랫폼 확인 (Linux 또는 Windows)
            response = self.ec2_client.describe_instances(InstanceIds=[instance_id])
            platform = response['Reservations'][0]['Instances'][0].get('PlatformDetails', 'Linux/UNIX').lower()
            
            if 'windows' in platform:
                # Windows: C: 드라이브 사용률 확인
                command = f"Get-PSDrive C | Select-Object @{{Name=\"UsedPercent\";Expression={{($_.Used / ($_.Used + $_.Free)) * 100}}}} | ConvertTo-Json"
                document_name = 'AWS-RunPowerShellScript'
            else:
                # Linux: 루트(/) 파티션 사용률 확인
                command = f"df -h / | awk 'NR==2 {{print $5}}' | sed 's/%//'"
                document_name = 'AWS-RunShellScript'
            
            logger.info(f"SSM Run Command 실행 (루트 디스크 사용률): 인스턴스 {instance_id}, 명령: {command}")
            
            response = self.ssm_client.send_command(
                InstanceIds=[instance_id],
                DocumentName=document_name,
                Parameters={'commands': [command]},
                TimeoutSeconds=300
            )
            command_id = response['Command']['CommandId']
            
            # 명령 실행 완료까지 대기 (최대 30초)
            for _ in range(6):
                time.sleep(5)
                output = self.ssm_client.get_command_invocation(
                    CommandId=command_id,
                    InstanceId=instance_id
                )
                if output['Status'] in ['Success', 'Failed', 'TimedOut', 'Cancelled']:
                    break
            else:
                logger.warning(f"SSM 명령(루트 디스크 사용률) {command_id} 실행 시간이 초과되었습니다.")
                return None
            
            if output['Status'] == 'Success':
                command_output = output['StandardOutputContent'].strip()
                logger.info(f"SSM 명령 실행 결과 (루트 디스크 사용률): {command_output}")
                
                if 'windows' in platform:
                    try:
                        # 결과가 단일 객체 또는 배열일 수 있음
                        # 예: {"UsedPercent":75.2} 또는 [{"UsedPercent":75.2}]
                        parsed_output = json.loads(command_output)
                        if isinstance(parsed_output, list):
                            used_percent = float(parsed_output[0]['UsedPercent'])
                        else:
                            used_percent = float(parsed_output['UsedPercent'])
                        return [{'Timestamp': datetime.now(), 'Average': used_percent, 'Unit': 'Percent'}]
                    except (json.JSONDecodeError, KeyError, IndexError) as e:
                        logger.error(f"SSM PowerShell 결과(루트 디스크) JSON 파싱 오류: {command_output}, 오류: {e}")
                        return None
                else:
                    if command_output.isdigit():
                        used_percent = float(command_output)
                        return [{'Timestamp': datetime.now(), 'Average': used_percent, 'Unit': 'Percent'}]
                    else:
                        logger.warning(f"SSM Shell 결과(루트 디스크)가 숫자가 아닙니다: {command_output}")
                        return None
            else:
                logger.error(f"SSM 명령(루트 디스크 사용률) {command_id} 실행 실패: {output['Status']} - {output['StandardErrorContent']}")
                return None
                
        except Exception as e:
            logger.error(f"SSM을 통한 루트 디스크 사용률 조회 중 오류 발생: {str(e)}", exc_info=True)
            return None
            
    def is_overprovisioned(self, usage_datapoints, current_size_gb):
        """ 
        주어진 사용률 데이터포인트와 현재 크기를 기준으로 과대 프로비저닝 여부 판단.
        ebs-optimization-tool_old 에서는 직접적인 디스크 사용률 기준은 없었고,
        주로 IOPS, Throughput 기준으로 판단했음.
        여기서는 storage_optimizer_by_metrics의 로직을 일부 차용.
        """
        if not usage_datapoints: # 사용률 데이터를 가져오지 못한 경우 (None 또는 빈 리스트)
            logger.warning("디스크 사용률 데이터가 없어 과대 프로비저닝(크기) 판단 불가.")
            # 반환 값: (과대 프로비저닝 여부, 사유, 사용률 요약, 권장 크기)
            return False, "사용률 데이터 없음", {}, None # 최적 크기 None

        # === 디버깅 로그 추가 시작 ===
        for i, dp in enumerate(usage_datapoints):
            avg_val = dp.get('Maximum', dp.get('Average', 0))
            logger.info(f"Datapoint {i} value: {avg_val}, type: {type(avg_val)}")
        # === 디버깅 로그 추가 끝 ===

        try:
            # 데이터 포인트가 여러 개일 수 있으므로, 가장 최근(혹은 최대) 값을 사용
            # Average 필드가 실제 사용률을 나타낸다고 가정 (SSM 결과와 CWAgent 결과 형식 통일 필요)
            # 여기서는 Maximum을 사용 (get_disk_usage_metrics에서 Maximum도 가져오도록 함)
            latest_usage_percent = max(dp.get('Maximum', dp.get('Average', 0)) for dp in usage_datapoints)
        except (ValueError, TypeError) as e:
            logger.error(f"사용률 데이터 파싱 중 오류 발생: {usage_datapoints}, error: {e}")
            return False, "사용률 데이터 파싱 오류", {}, None

        # === 디버깅 로그 추가 ===
        logger.info(f"Calculated latest_usage_percent: {latest_usage_percent}, type: {type(latest_usage_percent)}")
        criteria_low_usage_threshold = self.criteria.get('low_usage_threshold_percent', 20)
        logger.info(f"Criteria low_usage_threshold_percent: {criteria_low_usage_threshold}, type: {type(criteria_low_usage_threshold)}")
        # === 디버깅 로그 추가 끝 ===

        # 사용된 공간 (GB)
        used_gb = current_size_gb * (latest_usage_percent / 100.0)
        free_gb = current_size_gb - used_gb
        free_percent = (free_gb / current_size_gb) * 100 if current_size_gb > 0 else 0

        # 과대 프로비저닝 기준 (config.py의 OVERPROVISIONED_CRITERIA 에서 가져옴)
        # 1. 사용률이 N% 미만
        # 2. 여유 공간이 M GB 초과 (또는 여유 비율이 K% 초과)
        low_usage_threshold_percent = self.criteria.get('low_usage_threshold_percent', 20)
        min_free_space_gb_for_resize = self.criteria.get('min_free_space_gb_for_resize', 50) # 크기 조정 추천을 위한 최소 여유 공간
        # max_free_percent_for_resize = self.criteria.get('max_free_percent_for_resize', 80) # 크기 조정 추천을 위한 최대 여유 비율
        
        # === 디버깅 로그 추가 ===
        logger.info(f"Comparing latest_usage_percent ({latest_usage_percent}, type: {type(latest_usage_percent)}) with low_usage_threshold_percent ({low_usage_threshold_percent}, type: {type(low_usage_threshold_percent)})")
        # === 디버깅 로그 추가 끝 ===

        # 현재 사용률이 매우 낮은 경우 (예: 20% 미만)
        is_low_usage = latest_usage_percent < low_usage_threshold_percent
        
        # === 디버깅 로그 추가 ===
        logger.info(f"Comparing free_gb ({free_gb}, type: {type(free_gb)}) with min_free_space_gb_for_resize ({min_free_space_gb_for_resize}, type: {type(min_free_space_gb_for_resize)})")
        # === 디버깅 로그 추가 끝 ===
        # 여유 공간이 매우 큰 경우 (예: 50GB 초과)
        is_large_free_space = free_gb > min_free_space_gb_for_resize 
        #  and free_percent > max_free_percent_for_resize # 비율 조건도 추가 가능

        if is_low_usage and is_large_free_space:
            min_reduction_percent = self.criteria.get('min_reduction_percent_for_recommendation', 10)
            min_reduction_gb = self.criteria.get('min_reduction_gb_for_recommendation', 5)

            # === 디버깅 로그 추가 ===
            logger.info(f"Comparing current_size_gb - recommended_size_gb ({current_size_gb - recommended_size_gb}, type: {type(current_size_gb - recommended_size_gb)}) with min_reduction_gb ({min_reduction_gb}, type: {type(min_reduction_gb)})")
            logger.info(f"Comparing (current_size_gb - recommended_size_gb) / current_size_gb * 100 ({(current_size_gb - recommended_size_gb) / current_size_gb * 100 if current_size_gb else 0}, type: {type((current_size_gb - recommended_size_gb) / current_size_gb * 100 if current_size_gb else 0)}) with min_reduction_percent ({min_reduction_percent}, type: {type(min_reduction_percent)})")
            logger.info(f"Comparing recommended_size_gb ({recommended_size_gb}, type: {type(recommended_size_gb)}) with current_size_gb ({current_size_gb}, type: {type(current_size_gb)})")
            # === 디버깅 로그 추가 끝 ===

            if current_size_gb - recommended_size_gb >= min_reduction_gb and \
               (current_size_gb - recommended_size_gb) / current_size_gb * 100 >= min_reduction_percent and \
               recommended_size_gb < current_size_gb:
                return True, f"현재 사용률이 매우 낮은 경우 (예: {latest_usage_percent:.2f}%) 및 여유 공간이 매우 큰 경우 (예: {free_percent:.2f}%)로 인해 추천 크기를 {recommended_size_gb}GB로 조정합니다."
            else:
                return False, f"현재 사용률이 매우 낮은 경우 (예: {latest_usage_percent:.2f}%) 및 여유 공간이 매우 큰 경우 (예: {free_percent:.2f}%)로 인해 추천 크기를 {recommended_size_gb}GB로 조정합니다.", {}, recommended_size_gb
        else:
            return False, f"현재 사용률이 매우 낮은 경우 (예: {latest_usage_percent:.2f}%) 및 여유 공간이 매우 큰 경우 (예: {free_percent:.2f}%)로 인해 추천 크기를 {recommended_size_gb}GB로 조정합니다.", {}, recommended_size_gb

    def detect_overprovisioned_volumes(self, volumes):
        """
        과대 프로비저닝된 볼륨을 감지합니다.
        
        :param volumes: 분석할 볼륨 목록 (EC2 describe_volumes 결과)
        :return: 과대 프로비저닝된 볼륨 정보 리스트
        """
        overprovisioned_volumes = []
        end_time = datetime.now()
        # 'time_period_months' 또는 'days_to_check'를 사용하여 시작 시간 결정
        if 'time_period_months' in self.criteria:
            start_time = end_time - timedelta(days=self.criteria['time_period_months'] * 30)
        elif 'days_to_check' in self.criteria:
            start_time = end_time - timedelta(days=self.criteria['days_to_check'])
        else:
            start_time = end_time - timedelta(days=30) # 기본 30일

        for volume in volumes:
            volume_id = volume['VolumeId']
            logger.info(f"볼륨 {volume_id} 과대 프로비저닝 분석 시작...")

            # 볼륨 상태 확인 (예: 'available' 상태는 분석 제외)
            if volume.get('State') != 'in-use':
                logger.info(f"볼륨 {volume_id}은(는) 'in-use' 상태가 아니므로 과대 프로비저닝 분석에서 제외됩니다.")
                continue

            # 연결된 인스턴스 정보 가져오기
            attachments = volume.get('Attachments', [])
            if not attachments:
                logger.info(f"볼륨 {volume_id}은(는) 연결된 인스턴스가 없어 과대 프로비저닝 분석에서 제외됩니다.")
                continue
            
            # 첫 번째 연결된 인스턴스 정보 사용 (일반적으로 단일 연결)
            instance_id = attachments[0]['InstanceId']
            device_name = attachments[0]['Device']
            
            # 디스크 사용률 지표 가져오기
            # 이 함수는 CloudWatch 에이전트 메트릭 또는 SSM Run Command를 사용할 수 있습니다.
            usage_datapoints = self.get_disk_usage_metrics(instance_id, device_name, start_time, end_time)
            
            # 사용률 데이터를 가져오지 못한 경우 초기 분석 결과 반환
            if usage_datapoints is None:
                logger.warning(f"볼륨 {volume_id}의 디스크 사용률 데이터 없음. 크기 분석은 건너뛰기.")
                current_size = volume['Size']
                volume_type = volume['VolumeType']
                iops = volume.get('Iops')
                throughput = volume.get('Throughput')
                current_cost = calculate_monthly_cost(current_size, volume_type, self.region, iops, throughput)
                
                # 성능 분석은 시도 가능
                performance_metrics = self.get_performance_metrics(volume_id, start_time, end_time)
                is_perf_over, perf_reason = self.is_performance_overprovisioned(
                    performance_metrics, volume_type, iops, throughput
                )
                
                return {
                    'volume_id': volume_id,
                    'instance_id': instance_id,
                    'device_name': device_name,
                    'region': self.region,
                    'name': next((tag['Value'] for tag in volume.get('Tags', []) if tag['Key'] == 'Name'), 'N/A'),
                    'current_size_gb': current_size,
                    'volume_type': volume_type,
                    'current_iops': iops,
                    'current_throughput': throughput,
                    'current_monthly_cost': current_cost,
                    'disk_usage_status': 'unavailable',
                    'disk_usage_error_reason': 'Failed to retrieve disk usage from CWAgent and SSM.',
                    'disk_usage_data': {},
                    'is_size_overprovisioned': False,
                    'size_overprovisioned_reason': 'Disk usage data not available',
                    'recommended_size_gb': current_size, # 변경 권장 없음
                    'recommended_monthly_cost': current_cost, # 현재 비용과 동일
                    'estimated_monthly_savings': 0, # 크기 절감액 없음
                    'is_performance_overprovisioned': is_perf_over,
                    'performance_overprovisioned_reason': perf_reason,
                    'recommendation': f"디스크 사용량 정보를 가져올 수 없어 크기 최적화 권장은 제공되지 않습니다. {perf_reason if is_perf_over else '성능 문제는 발견되지 않았습니다.'}",
                    'is_overprovisioned': is_perf_over # 성능만으로 과대 프로비저닝 여부 판단
                }

            # is_overprovisioned 반환 값 변경: is_size_over, size_reason, usage_summary_from_is_over, recommended_size_from_is_over
            is_size_over, size_reason, usage_summary_from_is_over, recommended_size_from_is_over = self.is_overprovisioned(usage_datapoints, volume['Size'])
            
            # 디스크 사용률 데이터 요약
            avg_usage = 0
            num_dp = 0
            latest_usage = 0
            max_usage = 0
            if usage_datapoints:
                try:
                    avg_values = [dp.get('Average', 0) for dp in usage_datapoints]
                    avg_usage = sum(avg_values) / len(avg_values) if avg_values else 0
                    num_dp = len(usage_datapoints)
                    latest_usage = usage_datapoints[-1]['Average'] if usage_datapoints else 0
                    max_usage = max(dp['Average'] for dp in usage_datapoints) if usage_datapoints else 0
                except (TypeError, KeyError, IndexError) as e:
                    logger.error(f"볼륨 {volume_id}의 단일 분석 사용률 데이터 요약 중 오류: {e}, 데이터: {usage_datapoints}")

            usage_summary = {
                'average_usage_percent': avg_usage,
                'datapoints_count': num_dp,
                'collection_period_days': (end_time - start_time).days,
                'latest_usage_percent': latest_usage,
                'max_usage_percent': max_usage,
            }

            current_size = volume['Size']
            volume_type = volume['VolumeType']
            iops = volume.get('Iops')
            throughput = volume.get('Throughput')
            current_cost = calculate_monthly_cost(current_size, volume_type, self.region, iops, throughput)
            
            recommended_size = current_size
            recommended_cost = current_cost
            estimated_savings = 0
            final_recommendation = ""

            if is_size_over:
                logger.info(f"볼륨 {volume_id}이(가) 크기 면에서 과대 프로비저닝된 것으로 감지됨: {size_reason}")
                # is_overprovisioned에서 반환된 recommended_size 사용 또는 여기서 다시 계산
                # 여기서는 recommend_volume_size_and_cost를 다시 호출하여 일관성 유지
                temp_recommended_size, temp_recommended_cost = self.recommend_volume_size_and_cost(
                    usage_summary, current_size, volume_type, self.region, iops, throughput
                )
                if temp_recommended_size < current_size: # 축소 권장이 있을 경우에만 업데이트
                    recommended_size = temp_recommended_size
                    recommended_cost = temp_recommended_cost
                    estimated_savings = (current_cost - recommended_cost) if current_cost is not None and recommended_cost is not None else 0
                    final_recommendation = f"볼륨 크기를 {recommended_size}GB로 조정하여 월 ${estimated_savings:.2f} 절감 가능. {size_reason}"
                else:
                    # is_overprovisioned는 True를 반환했지만, recommend_volume_size_and_cost에서 축소 권장이 나오지 않은 경우
                    is_size_over = False # 실제로는 과대 프로비저닝이 아님 (또는 권장할 만큼 크지 않음)
                    size_reason = f"낮은 사용률에도 불구하고, 권장 크기({temp_recommended_size}GB)가 현재 크기({current_size}GB)보다 작지 않아 크기 조정 권장 안 함."
                    final_recommendation = size_reason
            else:
                logger.info(f"볼륨 {volume_id}은(는) 크기 면에서 과대 프로비저닝되지 않았습니다: {size_reason}")
                final_recommendation = size_reason
                
            # 성능 메트릭 기반 추가 분석 (IOPS, Throughput)
            # 성능 분석은 디스크 사용량 데이터 유무와 관계없이 수행 가능
            # is_perf_over, perf_reason = False, "성능 분석은 현재 비활성화됨" # 임시
            performance_metrics = self.get_performance_metrics(volume_id, start_time, end_time)
            is_perf_over, perf_reason = self.is_performance_overprovisioned(performance_metrics, volume_type, iops, throughput)
            
            if is_perf_over:
                logger.info(f"볼륨 {volume_id}이(가) 성능 면에서 과대 프로비저닝된 것으로 감지됨: {perf_reason}")
                if final_recommendation and not final_recommendation.startswith("현재 사용률이") : # 이미 크기 관련 메시지가 있으면 추가
                    final_recommendation += f" 또한, {perf_reason}"
                else: # 크기 관련 메시지가 없거나, 판단 불가 메시지면 새로 작성
                    final_recommendation = perf_reason
                # 성능 최적화 권장 (예: gp3로 변경, IOPS/처리량 조정)은 여기서 구체화 가능
            
            # 최종 결과 객체 구성
            result_item = {
                'volume_id': volume_id,
                'instance_id': instance_id,
                'device_name': device_name,
                'region': self.region,
                'name': next((tag['Value'] for tag in volume.get('Tags', []) if tag['Key'] == 'Name'), 'N/A'),
                'current_size_gb': current_size,
                'volume_type': volume_type,
                'current_iops': iops,
                'current_throughput': throughput,
                'current_monthly_cost': current_cost,
                'disk_usage_status': 'available' if usage_datapoints else 'unavailable',
                'disk_usage_error_reason': None if usage_datapoints else 'Failed to retrieve disk usage from CWAgent and SSM.',
                'disk_usage_data': usage_summary if usage_datapoints else {},
                'overprovisioned_reason': size_reason if usage_datapoints else 'Disk usage data not available',
                'recommended_size_gb': recommended_size if is_size_over and usage_datapoints else current_size,
                'recommended_monthly_cost': recommended_cost if is_size_over and usage_datapoints else current_cost,
                'estimated_monthly_savings': round(estimated_savings, 2) if is_size_over and usage_datapoints and estimated_savings > 0 else 0,
                'is_size_overprovisioned': is_size_over and usage_datapoints, # 사용량 데이터가 있어야 크기 과대프로비저닝 판단 가능
                'is_performance_overprovisioned': is_perf_over,
                'performance_overprovisioned_reason': perf_reason,
                'recommendation': final_recommendation if final_recommendation else "분석 결과 특이사항 없음.",
                'is_overprovisioned': (is_size_over and usage_datapoints) or is_perf_over # 최종 과대 프로비저닝 여부
            }
            overprovisioned_volumes.append(result_item)
            
        return overprovisioned_volumes

    def recommend_volume_size_and_cost(self, usage_summary, current_size, volume_type, region, current_iops=None, current_throughput=None):
        """
        과대 프로비저닝된 볼륨에 대한 권장 크기 및 비용을 계산합니다.
        
        :param usage_summary: 디스크 사용률 요약 정보 (average_usage_percent 포함)
        :param current_size: 현재 볼륨 크기 (GB)
        :param volume_type: 볼륨 유형
        :param region: AWS 리전
        :param current_iops: 현재 IOPS (gp3, io1, io2용)
        :param current_throughput: 현재 처리량 (gp3용)
        :return: (권장 크기, 권장 월간 비용)
        """
        avg_usage_percent = usage_summary.get('average_usage_percent', 0)
        if avg_usage_percent == 0: # 사용률이 0이면 최소 크기로 조정 (예: 1GB 또는 구성된 최소값)
            # 최소 크기는 볼륨 유형이나 OS 요구 사항에 따라 다를 수 있음
            # 여기서는 단순하게 1GB로 가정하거나, 설정된 최소 버퍼 크기를 사용
            recommended_size = max(1, self.criteria.get('resize_min_buffer_gb', 10) if self.criteria.get('resize_min_buffer_gb', 10) > 0 else 1)
            logger.info(f"평균 사용률이 0%이므로 최소 권장 크기 {recommended_size}GB 적용.")
        else:
            # 사용된 공간 계산 (GB)
            used_space_gb = current_size * (avg_usage_percent / 100.0)
            
            # 버퍼 추가 (설정된 비율 또는 최소 버퍼 크기 중 큰 값)
            buffer_percent = self.criteria.get('buffer_percent', 20) / 100.0 # 예: 20% -> 0.2
            min_buffer_gb = self.criteria.get('resize_min_buffer_gb', 10) # 예: 10GB
            
            # 버퍼 크기 계산
            buffer_from_percent = used_space_gb * buffer_percent
            final_buffer_gb = max(buffer_from_percent, min_buffer_gb)
            
            # 권장 크기 계산 (사용된 공간 + 버퍼)
            recommended_size_raw = used_space_gb + final_buffer_gb
            
            # AWS EBS 최소 크기(1GB) 및 정수 단위 적용
            recommended_size = max(1, int(round(recommended_size_raw)))
            logger.info(f"권장 크기 계산: 사용 공간={used_space_gb:.2f}GB, 버퍼={final_buffer_gb:.2f}GB (비율 기반: {buffer_from_percent:.2f}GB, 최소: {min_buffer_gb}GB), 원시 권장={recommended_size_raw:.2f}GB, 최종={recommended_size}GB")

        # 권장 크기가 현재 크기보다 크거나 같으면 변경하지 않음 (축소만 권장)
        if recommended_size >= current_size:
            logger.info(f"권장 크기({recommended_size}GB)가 현재 크기({current_size}GB)보다 크거나 같으므로 크기 조정을 권장하지 않습니다.")
            return current_size, calculate_monthly_cost(current_size, volume_type, region, current_iops, current_throughput)

        # 권장 크기에 대한 월간 비용 계산
        # IOPS와 처리량은 현재 값을 그대로 사용한다고 가정 (타입 변경은 별도 로직)
        recommended_cost = calculate_monthly_cost(recommended_size, volume_type, region, current_iops, current_throughput)
        
        return recommended_size, recommended_cost
        
    # get_performance_metrics, is_performance_overprovisioned 등은 추가 구현 필요
    # 여기서는 디스크 공간 기반의 과대 프로비저닝에 중점

    def get_performance_metrics(self, volume_id, start_time, end_time):
        """
        볼륨의 성능 관련 CloudWatch 메트릭(IOPS, 처리량)을 수집합니다.
        
        :param volume_id: EBS 볼륨 ID
        :param start_time: 수집 시작 시간
        :param end_time: 수집 종료 시간
        :return: 수집된 성능 메트릭 딕셔너리
        """
        metrics_data = {}
        # 수집할 성능 메트릭 목록
        performance_metric_names = [
            'VolumeReadOps', 'VolumeWriteOps', # 합쳐서 총 IOPS 계산
            'VolumeReadBytes', 'VolumeWriteBytes' # 합쳐서 총 처리량 계산
        ]

        for metric_name in performance_metric_names:
            try:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace='AWS/EBS',
                    MetricName=metric_name,
                    Dimensions=[{'Name': 'VolumeId', 'Value': volume_id}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=self.criteria.get('metric_period_seconds', 86400), # 일별 평균 권장
                    Statistics=['Average', 'Maximum', 'Sum'] # Sum은 기간 동안의 총량, Average는 기간 평균
                )
                if response['Datapoints']:
                    # 모든 데이터포인트 저장 또는 요약 정보만 저장
                    # 여기서는 단순화를 위해 평균값과 최대값을 주로 사용
                    avg_values = [dp['Average'] for dp in response['Datapoints']]
                    max_values = [dp['Maximum'] for dp in response['Datapoints']]
                    
                    metrics_data[metric_name] = {
                        'average': sum(avg_values) / len(avg_values) if avg_values else 0,
                        'maximum': max(max_values) if max_values else 0,
                        'unit': response['Datapoints'][0]['Unit'],
                        'datapoints': response['Datapoints'] # 원본 데이터
                    }
            except Exception as e:
                logger.warning(f"볼륨 {volume_id}의 성능 메트릭 {metric_name} 조회 중 오류 발생: {str(e)}")
        
        # 집계된 IOPS 및 처리량 계산
        # IOPS: 초당 작업 수 (Ops/Second)
        # 처리량: 초당 바이트 수 (Bytes/Second), MiBps로 변환 필요
        if 'VolumeReadOps' in metrics_data and 'VolumeWriteOps' in metrics_data:
            avg_read_ops = metrics_data['VolumeReadOps'].get('average', 0)
            avg_write_ops = metrics_data['VolumeWriteOps'].get('average', 0)
            max_read_ops = metrics_data['VolumeReadOps'].get('maximum', 0)
            max_write_ops = metrics_data['VolumeWriteOps'].get('maximum', 0)
            
            # CloudWatch 메트릭은 Period 동안의 평균/최대 값. IOPS는 초당 값.
            # Period가 86400초(1일)인 경우, Average(Op/s) = Sum(Ops) / 86400
            # get_metric_statistics의 Average는 이미 초당 평균임.
            metrics_data['TotalIOPS'] = {
                'average': avg_read_ops + avg_write_ops,
                'maximum': max_read_ops + max_write_ops, # 최대값은 합산이 아니라 동시 발생 최대를 봐야 함
                                                      # 여기서는 단순 합으로 계산 (보수적 접근)
                'unit': 'Ops/Second'
            }

        if 'VolumeReadBytes' in metrics_data and 'VolumeWriteBytes' in metrics_data:
            avg_read_bytes = metrics_data['VolumeReadBytes'].get('average', 0) # Bytes/Second
            avg_write_bytes = metrics_data['VolumeWriteBytes'].get('average', 0) # Bytes/Second
            max_read_bytes = metrics_data['VolumeReadBytes'].get('maximum', 0)
            max_write_bytes = metrics_data['VolumeWriteBytes'].get('maximum', 0)
            
            # Bytes/Second to MiBps (1 MiB = 1024 * 1024 Bytes)
            total_avg_mibps = (avg_read_bytes + avg_write_bytes) / (1024 * 1024)
            total_max_mibps = (max_read_bytes + max_write_bytes) / (1024 * 1024) # 최대값 단순 합
            
            metrics_data['TotalThroughputMiBps'] = {
                'average': total_avg_mibps,
                'maximum': total_max_mibps,
                'unit': 'MiB/s'
            }
            
        return metrics_data

    def is_performance_overprovisioned(self, performance_metrics, volume_type, provisioned_iops, provisioned_throughput_mibps):
        """
        성능 메트릭(IOPS, 처리량)을 기반으로 볼륨이 과대 프로비저닝되었는지 확인합니다.
        
        :param performance_metrics: get_performance_metrics에서 반환된 딕셔너리
        :param volume_type: 볼륨 유형 (gp3, io1, io2 등)
        :param provisioned_iops: 프로비저닝된 IOPS
        :param provisioned_throughput_mibps: 프로비저닝된 처리량 (MiBps)
        :return: (과대 프로비저닝 여부, 판단 근거 메시지)
        """
        reasons = []
        # IOPS 및 처리량 사용률 임계값 (config.py 에서 가져옴)
        iops_usage_threshold_percent = self.criteria.get('iops_usage_threshold_percent', 20) # 기본 20%
        throughput_usage_threshold_percent = self.criteria.get('throughput_usage_threshold_percent', 20) # 기본 20%

        # IOPS 분석 (gp3, io1, io2에 해당)
        if volume_type in ['gp3', 'io1', 'io2'] and provisioned_iops and 'TotalIOPS' in performance_metrics:
            avg_iops = performance_metrics['TotalIOPS'].get('average', 0)
            max_iops = performance_metrics['TotalIOPS'].get('maximum', 0)
            # 사용률 계산 시 평균 IOPS 또는 최대 IOPS 중 어떤 것을 기준으로 할지 결정 필요
            # 여기서는 평균 IOPS 사용
            iops_utilization = (avg_iops / provisioned_iops) * 100 if provisioned_iops > 0 else 0
            
            logger.info(f"IOPS 분석: 평균 사용 IOPS={avg_iops:.2f}, 최대 사용 IOPS={max_iops:.2f}, 프로비저닝된 IOPS={provisioned_iops}, 사용률={iops_utilization:.2f}%")

            if iops_utilization < iops_usage_threshold_percent:
                reasons.append(f"평균 IOPS 사용률({iops_utilization:.2f}%)이 임계값({iops_usage_threshold_percent}%) 미만입니다.")
        
        # 처리량 분석 (gp3에 해당, 다른 타입은 스토리지 크기에 따라 결정되므로 직접 비교 어려움)
        if volume_type == 'gp3' and provisioned_throughput_mibps and 'TotalThroughputMiBps' in performance_metrics:
            avg_throughput = performance_metrics['TotalThroughputMiBps'].get('average', 0)
            max_throughput = performance_metrics['TotalThroughputMiBps'].get('maximum', 0)
            # 평균 처리량 사용
            throughput_utilization = (avg_throughput / provisioned_throughput_mibps) * 100 if provisioned_throughput_mibps > 0 else 0
            
            logger.info(f"처리량 분석: 평균 사용 처리량={avg_throughput:.2f} MiBps, 최대 사용 처리량={max_throughput:.2f} MiBps, 프로비저닝된 처리량={provisioned_throughput_mibps} MiBps, 사용률={throughput_utilization:.2f}%")

            if throughput_utilization < throughput_usage_threshold_percent:
                reasons.append(f"평균 처리량 사용률({throughput_utilization:.2f}%)이 임계값({throughput_usage_threshold_percent}%) 미만입니다.")

        if reasons:
            # 하나 이상의 성능 지표가 과대 프로비저닝된 경우
            return True, " / ".join(reasons)
        else:
            return False, "성능(IOPS/처리량)은 과대 프로비저닝되지 않았습니다."

    # is_overprovisioned_volume 메서드는 detect_overprovisioned_volumes 내부 로직과 유사하므로,
    # detect_overprovisioned_volumes 로 통합하거나, 더 세분화된 단일 볼륨 분석 함수로 유지할 수 있음.
    # 현재는 detect_overprovisioned_volumes가 주된 분석 함수로 사용됨.
    def is_overprovisioned_volume(self, volume_id, volume):
        """
        단일 볼륨에 대해 과대 프로비저닝 여부를 판단합니다.
        detect_overprovisioned_volumes의 내부 로직과 유사하지만, 단일 볼륨에 대한 상세 반환을 위함.
        
        :param volume_id: 분석할 볼륨 ID
        :param volume: EC2 describe_volumes 결과의 단일 볼륨 객체
        :return: 과대 프로비저닝 분석 결과 딕셔너리 또는 None (분석 불가 시)
        """
        logger.info(f"단일 볼륨 {volume_id} 과대 프로비저닝 분석 시작...")
        end_time = datetime.now()
        if 'time_period_months' in self.criteria:
            start_time = end_time - timedelta(days=self.criteria['time_period_months'] * 30)
        elif 'days_to_check' in self.criteria:
            start_time = end_time - timedelta(days=self.criteria['days_to_check'])
        else:
            start_time = end_time - timedelta(days=30)

        if volume.get('State') != 'in-use':
            logger.info(f"볼륨 {volume_id}은(는) 'in-use' 상태가 아님. 분석 건너뛰기.")
            return None

        attachments = volume.get('Attachments', [])
        if not attachments:
            logger.info(f"볼륨 {volume_id}에 연결된 인스턴스가 없어 과대 프로비저닝 분석에서 제외됩니다.")
            return None
        
        instance_id = attachments[0]['InstanceId']
        device_name = attachments[0]['Device']
        
        usage_datapoints = self.get_disk_usage_metrics(instance_id, device_name, start_time, end_time)
        
        # 사용률 데이터를 가져오지 못한 경우 초기 분석 결과 반환
        if usage_datapoints is None:
            logger.warning(f"볼륨 {volume_id}의 디스크 사용률 데이터 없음. 크기 분석은 건너뛰기.")
            current_size = volume['Size']
            volume_type = volume['VolumeType']
            iops = volume.get('Iops')
            throughput = volume.get('Throughput')
            current_cost = calculate_monthly_cost(current_size, volume_type, self.region, iops, throughput)
            
            # 성능 분석은 시도 가능
            performance_metrics = self.get_performance_metrics(volume_id, start_time, end_time)
            is_perf_over, perf_reason = self.is_performance_overprovisioned(
                performance_metrics, volume_type, iops, throughput
            )
            
            return {
                'volume_id': volume_id,
                'instance_id': instance_id,
                'device_name': device_name,
                'region': self.region,
                'name': next((tag['Value'] for tag in volume.get('Tags', []) if tag['Key'] == 'Name'), 'N/A'),
                'current_size_gb': current_size,
                'volume_type': volume_type,
                'current_iops': iops,
                'current_throughput': throughput,
                'current_monthly_cost': current_cost,
                'disk_usage_status': 'unavailable',
                'disk_usage_error_reason': 'Failed to retrieve disk usage from CWAgent and SSM.',
                'disk_usage_data': {},
                'is_size_overprovisioned': False,
                'size_overprovisioned_reason': 'Disk usage data not available',
                'recommended_size_gb': current_size, # 변경 권장 없음
                'recommended_monthly_cost': current_cost, # 현재 비용과 동일
                'estimated_monthly_savings': 0, # 크기 절감액 없음
                'is_performance_overprovisioned': is_perf_over,
                'performance_overprovisioned_reason': perf_reason,
                'recommendation': f"디스크 사용량 정보를 가져올 수 없어 크기 최적화 권장은 제공되지 않습니다. {perf_reason if is_perf_over else '성능 문제는 발견되지 않았습니다.'}",
                'is_overprovisioned': is_perf_over # 성능만으로 과대 프로비저닝 여부 판단
            }

        # is_overprovisioned 반환 값 변경: is_size_over, size_reason, usage_summary_from_is_over, recommended_size_from_is_over
        is_size_over, size_reason, usage_summary_from_is_over, recommended_size_from_is_over = self.is_overprovisioned(usage_datapoints, volume['Size'])
        
        # 디스크 사용률 데이터 요약
        avg_usage = 0
        num_dp = 0
        latest_usage = 0
        max_usage = 0
        if usage_datapoints:
            try:
                avg_values = [dp.get('Average', 0) for dp in usage_datapoints]
                avg_usage = sum(avg_values) / len(avg_values) if avg_values else 0
                num_dp = len(usage_datapoints)
                latest_usage = usage_datapoints[-1]['Average'] if usage_datapoints else 0
                max_usage = max(dp['Average'] for dp in usage_datapoints) if usage_datapoints else 0
            except (TypeError, KeyError, IndexError) as e:
                logger.error(f"볼륨 {volume_id}의 단일 분석 사용률 데이터 요약 중 오류: {e}, 데이터: {usage_datapoints}")

        usage_summary = {
            'average_usage_percent': avg_usage,
            'datapoints_count': num_dp,
            'collection_period_days': (end_time - start_time).days,
            'latest_usage_percent': latest_usage,
            'max_usage_percent': max_usage,
        }

        current_size = volume['Size']
        volume_type = volume['VolumeType']
        iops = volume.get('Iops')
        throughput = volume.get('Throughput')
        current_cost = calculate_monthly_cost(current_size, volume_type, self.region, iops, throughput)
        
        # 성능 과대 프로비저닝 분석
        # performance_metrics = self.get_performance_metrics(volume_id, start_time, end_time)
        # is_perf_over, perf_reason = self.is_performance_overprovisioned(
        #     performance_metrics, volume_type, iops, throughput
        # )
        is_perf_over, perf_reason = False, "성능 분석은 현재 비활성화됨" # 임시
        
        analysis_result = {
            'volume_id': volume_id,
            'instance_id': instance_id,
            'device_name': device_name,
            'region': self.region,
            'name': next((tag['Value'] for tag in volume.get('Tags', []) if tag['Key'] == 'Name'), 'N/A'),
            'current_size_gb': current_size,
            'volume_type': volume_type,
            'current_iops': iops,
            'current_throughput': throughput,
            'current_monthly_cost': current_cost,
            'disk_usage_status': 'available' if usage_datapoints else 'unavailable',
            'disk_usage_error_reason': None if usage_datapoints else 'Failed to retrieve disk usage from CWAgent and SSM.',
            'disk_usage_data': usage_summary if usage_datapoints else {},
            'is_size_overprovisioned': is_size_over,
            'size_overprovisioned_reason': size_reason if is_size_over else "크기 면에서는 과대 프로비저닝되지 않았음",
            'is_performance_overprovisioned': is_perf_over,
            'performance_overprovisioned_reason': perf_reason if is_perf_over else "N/A",
            'recommendation': "",
            'estimated_monthly_savings': 0
        }

        if is_size_over:
            recommended_size, recommended_cost = self.recommend_volume_size_and_cost(
                usage_summary, current_size, volume_type, self.region, iops, throughput
            )
            estimated_savings = (current_cost - recommended_cost) if current_cost is not None and recommended_cost is not None else 0
            
            analysis_result['recommended_size_gb'] = recommended_size
            analysis_result['recommended_monthly_cost'] = recommended_cost
            analysis_result['estimated_monthly_savings'] = round(estimated_savings, 2) if estimated_savings > 0 else 0
            analysis_result['recommendation'] = f"디스크 크기를 {recommended_size}GB로 조정하여 월 ${estimated_savings:.2f} 절감 가능"
        
        # 성능 최적화 권장 사항 추가 (필요 시)
        # if is_perf_over:
        #     # ... (gp3로 변경 또는 IOPS/처리량 조정 권장 로직)
        #     analysis_result['recommendation'] += " 성능 최적화 권장..."
            
        # 최종적으로 과대 프로비저닝으로 판단되는 경우 (크기 또는 성능 중 하나라도 해당)
        analysis_result['is_overprovisioned'] = is_size_over or is_perf_over
        if not analysis_result['is_overprovisioned']:
            analysis_result['recommendation'] = "현재 과대 프로비저닝된 것으로 보이지 않습니다."
            
        return analysis_result 