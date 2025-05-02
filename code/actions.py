import boto3
import logging
import time
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger()
# Lambda 환경에서는 기본 로거 설정이 다를 수 있으므로, 필요 시 핸들러 추가 고려
# logger.setLevel(logging.INFO)

class EBSActionExecutor:
    """
    EBS 볼륨에 대한 실제 조치(액션)를 수행하는 클래스
    분석 결과에 따른 권장 조치를 실행합니다.
    """

    def __init__(self, region):
        """
        :param region: AWS 리전
        """
        self.region = region
        self.ec2_client = boto3.client('ec2', region_name=region)

    def create_snapshot(self, volume_id, description=None, tags=None):
        """
        EBS 볼륨의 스냅샷을 생성합니다.
        스냅샷 생성은 비동기적으로 처리됩니다 - 생성 요청만 전송하고 완료를 기다리지 않습니다.

        :param volume_id: 스냅샷을 생성할 볼륨 ID
        :param description: 스냅샷 설명 (기본값: None)
        :param tags: 스냅샷에 적용할 태그 딕셔너리 (기본값: None)
        :return: 생성된 스냅샷 ID 또는 None (실패 시)
        """
        try:
            # 스냅샷 생성 요청 구성
            create_args = {'VolumeId': volume_id}

            if description:
                create_args['Description'] = description

            # 태그 변환
            if tags:
                tag_specs = [{
                    'ResourceType': 'snapshot',
                    'Tags': [{'Key': k, 'Value': v} for k, v in tags.items()]
                }]
                create_args['TagSpecifications'] = tag_specs

            logger.info(f"볼륨 {volume_id}의 스냅샷 생성 시작")
            response = self.ec2_client.create_snapshot(**create_args)

            snapshot_id = response.get('SnapshotId')
            logger.info(f"볼륨 {volume_id}의 스냅샷 {snapshot_id} 생성 요청 완료. 스냅샷 생성은 백그라운드에서 계속됩니다.")

            # 스냅샷 생성이 시작되었는지 확인하기 위한 간단한 시도 (선택 사항)
            # 너무 오래 기다리지 않도록 주의
            # try:
            #     waiter = self.ec2_client.get_waiter('snapshot_completed')
            #     waiter.wait(SnapshotIds=[snapshot_id], WaiterConfig={'Delay': 5, 'MaxAttempts': 1})
            #     logger.info(f"스냅샷 {snapshot_id} 생성이 진행 중입니다.")
            # except WaiterError:
            #     logger.info(f"스냅샷 {snapshot_id} 생성 확인 시간이 초과되었습니다. 백그라운드에서 계속 진행됩니다.")
            # except Exception as e:
            #     logger.warning(f"스냅샷 {snapshot_id} 상태 확인 중 문제 발생: {str(e)}")

            return snapshot_id

        except ClientError as e:
            logger.error(f"스냅샷 생성 중 오류 발생: {str(e)}")
            return None

    def detach_volume(self, volume_id, force=False):
        """
        EBS 볼륨을 인스턴스에서 분리합니다.
        분리 요청만 보내고 완료를 기다리지 않습니다.

        :param volume_id: 분리할 볼륨 ID
        :param force: 강제 분리 여부 (기본값: False)
        :return: 성공 여부 (boolean)
        """
        try:
            # 볼륨 정보 가져오기
            response = self.ec2_client.describe_volumes(VolumeIds=[volume_id])

            if not response['Volumes']:
                logger.error(f"볼륨 {volume_id}을 찾을 수 없습니다.")
                return False

            volume = response['Volumes'][0]
            attachments = volume.get('Attachments', [])

            # 연결된 인스턴스가 없으면 성공으로 처리
            if not attachments:
                logger.info(f"볼륨 {volume_id}이 어떤 인스턴스에도 연결되어 있지 않습니다.")
                return True

            # 각 연결에 대해 분리 실행 (첫 번째 연결만 처리하거나, 모든 연결을 처리할지 결정 필요)
            # 여기서는 첫 번째 연결만 처리 (일반적인 경우)
            attachment = attachments[0]
            instance_id = attachment.get('InstanceId')
            device = attachment.get('Device')

            logger.info(f"볼륨 {volume_id}을 인스턴스 {instance_id}의 {device}에서 분리 시도")

            detach_args = {
                'VolumeId': volume_id,
                'InstanceId': instance_id, # detach_volume은 InstanceId도 필요
                'Device': device           # detach_volume은 Device도 필요
            }

            if force:
                detach_args['Force'] = True

            self.ec2_client.detach_volume(**detach_args)
            logger.info(f"볼륨 {volume_id} 분리 요청 완료. 분리는 백그라운드에서 계속됩니다.")

            return True

        except ClientError as e:
            # 이미 분리 중이거나 분리된 상태일 수 있음 (Idempotency 고려)
            if e.response['Error']['Code'] == 'IncorrectState':
                 logger.warning(f"볼륨 {volume_id} 분리 시도 중 상태 오류: {e}. 이미 분리되었거나 진행 중일 수 있습니다.")
                 return True # 이미 원하는 상태일 수 있으므로 성공으로 간주
            logger.error(f"볼륨 분리 중 오류 발생: {str(e)}")
            return False

    def attach_volume(self, volume_id, instance_id, device):
        """
        EBS 볼륨을 인스턴스에 연결합니다.
        연결 요청을 보내고 완료되기를 기다리지 않습니다.

        :param volume_id: 연결할 볼륨 ID
        :param instance_id: 인스턴스 ID
        :param device: 디바이스 이름 (e.g., /dev/sdf)
        :return: 성공 여부 (boolean)
        """
        try:
            # 볼륨 상태 확인
            volume_info = self._get_volume_info(volume_id)
            if not volume_info:
                return False # 에러 로깅은 _get_volume_info 내부에서 처리
            if volume_info['state'] != 'available':
                logger.error(f"볼륨 {volume_id}의 상태가 'available'이 아닙니다: {volume_info['state']}")
                return False

            # 인스턴스 상태 확인 (필요 시)
            # ... (구현 추가)

            # 볼륨 연결
            logger.info(f"볼륨 {volume_id}를 인스턴스 {instance_id}에 연결합니다. (디바이스: {device})")

            self.ec2_client.attach_volume(
                VolumeId=volume_id,
                InstanceId=instance_id,
                Device=device
            )

            logger.info(f"볼륨 {volume_id}의 인스턴스 {instance_id} 연결 요청 완료. 연결은 백그라운드에서 계속됩니다.")
            return True

        except ClientError as e:
            logger.error(f"볼륨 연결 중 오류 발생: {str(e)}")
            return False

    def delete_volume(self, volume_id):
        """
        EBS 볼륨을 삭제합니다.
        삭제 요청만 보내고 완료를 기다리지 않습니다.

        :param volume_id: 삭제할 볼륨 ID
        :return: 성공 여부 (boolean)
        """
        try:
            # 볼륨 상태 확인 (삭제 가능한 상태인지)
            volume_info = self._get_volume_info(volume_id)
            if not volume_info:
                # 이미 삭제되었을 수 있음
                logger.warning(f"볼륨 {volume_id} 정보를 찾을 수 없어 삭제를 건너<0xEB><0x9A><0x95>니다 (이미 삭제되었을 수 있음).")
                return True # Idempotency: 이미 삭제된 경우 성공으로 간주

            if volume_info['state'] == 'in-use':
                logger.error(f"볼륨 {volume_id}가 사용 중({volume_info['state']})이므로 삭제할 수 없습니다. 먼저 분리해야 합니다.")
                return False

            logger.info(f"볼륨 {volume_id} 삭제 시작")
            self.ec2_client.delete_volume(VolumeId=volume_id)
            logger.info(f"볼륨 {volume_id} 삭제 요청 완료. 삭제는 백그라운드에서 계속됩니다.")

            return True

        except ClientError as e:
            # 이미 삭제 중이거나 삭제된 상태일 수 있음
            if e.response['Error']['Code'] == 'InvalidVolume.NotFound':
                logger.warning(f"볼륨 {volume_id} 삭제 시도 중 찾을 수 없음: {e}. 이미 삭제되었을 수 있습니다.")
                return True # 이미 원하는 상태
            logger.error(f"볼륨 삭제 중 오류 발생: {str(e)}")
            return False

    def modify_volume_type(self, volume_id, target_type, iops=None, throughput=None):
        """
        볼륨 유형을 변경합니다.

        :param volume_id: 볼륨 ID
        :param target_type: 대상 볼륨 타입
        :param iops: IOPS 값 (io1, io2, gp3 타입에만 필요)
        :param throughput: 처리량 (gp3 타입에만 필요)
        :return: 결과 딕셔너리 {'success': bool, 'message': str, ...}
        """
        try:
            logger.info(f"볼륨 {volume_id}의 타입을 {target_type}으로 변경 시작")

            # 현재 볼륨 정보 가져오기
            current_volume = self._get_volume_info(volume_id)
            if not current_volume:
                return {'success': False, 'error': f"볼륨 {volume_id} 정보를 가져올 수 없습니다."}

            current_type = current_volume.get('volume_type') # _get_volume_info 반환값 키 사용

            if current_type == target_type:
                logger.info(f"볼륨 {volume_id}이 이미 요청한 타입({target_type})입니다.")
                return {'success': True, 'message': f"볼륨 {volume_id}이 이미 요청한 타입({target_type})입니다."}

            # 변경 요청 준비
            modify_args = {
                'VolumeId': volume_id,
                'VolumeType': target_type
            }

            # 볼륨 타입에 따른 추가 파라미터 설정
            # 현재 값 또는 기본값을 설정하여 API 오류 방지
            current_iops = current_volume.get('iops')
            current_throughput = current_volume.get('throughput')

            if target_type in ['io1', 'io2']:
                modify_args['Iops'] = iops if iops is not None else (current_iops if current_iops is not None else 100)
            elif target_type == 'gp3':
                modify_args['Iops'] = iops if iops is not None else (current_iops if current_iops is not None else 3000)
                modify_args['Throughput'] = throughput if throughput is not None else (current_throughput if current_throughput is not None else 125)

            # 변경 요청
            response = self.ec2_client.modify_volume(**modify_args)

            # 변경 상태 확인 (API 호출 성공 여부만 확인)
            modification = response.get('VolumeModification', {})
            logger.info(f"볼륨 타입 변경 요청 완료. 변경은 백그라운드에서 계속됩니다. Modification details: {modification}")

            return {
                'success': True,
                'message': f"볼륨 타입 변경 요청 성공: {current_type} -> {target_type}",
                'modification_details': modification
            }

        except ClientError as e:
            logger.error(f"볼륨 타입 변경 중 오류 발생: {str(e)}")
            return {'success': False, 'error': str(e)}

    def modify_volume_size(self, volume_id, target_size):
        """
        볼륨 크기를 변경합니다. (크기 증가만 지원)

        :param volume_id: 볼륨 ID
        :param target_size: 대상 크기 (GB)
        :return: 결과 딕셔너리 {'success': bool, 'message': str, ...}
        """
        try:
            logger.info(f"볼륨 {volume_id}의 크기를 {target_size}GB로 변경 시작")

            # 현재 볼륨 정보 가져오기
            current_volume = self._get_volume_info(volume_id)
            if not current_volume:
                return {'success': False, 'error': f"볼륨 {volume_id} 정보를 가져올 수 없습니다."}

            current_size = current_volume.get('size')

            if current_size is None:
                 return {'success': False, 'error': f"볼륨 {volume_id}의 현재 크기를 알 수 없습니다."}

            if target_size == current_size:
                logger.info(f"볼륨 {volume_id}이 이미 요청한 크기({target_size}GB)입니다.")
                return {'success': True, 'message': f"볼륨 {volume_id}이 이미 요청한 크기({target_size}GB)입니다."}

            # 크기 축소는 지원하지 않음 (API 레벨에서 막힐 수 있음)
            if target_size < current_size:
                msg = f"볼륨 {volume_id}의 크기 축소({current_size}GB -> {target_size}GB)는 지원되지 않습니다."
                logger.error(msg)
                return {'success': False, 'error': msg}

            # 변경 요청 준비
            modify_args = {
                'VolumeId': volume_id,
                'Size': target_size
            }

            # 변경 요청
            response = self.ec2_client.modify_volume(**modify_args)

            modification = response.get('VolumeModification', {})
            logger.info(f"볼륨 크기 변경 요청 완료. 변경은 백그라운드에서 계속됩니다. Modification details: {modification}")

            return {
                'success': True,
                'message': f"볼륨 크기 변경 요청 성공: {current_size}GB -> {target_size}GB",
                'modification_details': modification
            }

        except ClientError as e:
            logger.error(f"볼륨 크기 변경 중 오류 발생: {str(e)}")
            return {'success': False, 'error': str(e)}

    def modify_volume(self, volume_id, target_type=None, target_size=None, iops=None, throughput=None):
        """
        볼륨 속성(타입, 크기, IOPS, 처리량)을 변경합니다.
        크기 증가는 가능하지만 축소는 불가능합니다.

        :param volume_id: 볼륨 ID
        :param target_type: 대상 볼륨 타입 (None이면 변경 안 함)
        :param target_size: 대상 크기 (GB) (None이면 변경 안 함)
        :param iops: IOPS 값 (io1, io2, gp3 타입 변경 시 필요할 수 있음)
        :param throughput: 처리량 (gp3 타입 변경 시 필요할 수 있음)
        :return: 결과 딕셔너리 {'success': bool, 'message': str, ...}
        """
        try:
            logger.info(f"볼륨 {volume_id} 속성 변경 시작 (Type: {target_type}, Size: {target_size}GB)")

            current_volume = self._get_volume_info(volume_id)
            if not current_volume:
                return {'success': False, 'error': f"볼륨 {volume_id} 정보를 가져올 수 없습니다."}

            current_type = current_volume.get('volume_type')
            current_size = current_volume.get('size')
            current_iops = current_volume.get('iops')
            current_throughput = current_volume.get('throughput')

            modify_args = {'VolumeId': volume_id}
            changes_requested = False

            # 타입 변경 처리
            if target_type and target_type != current_type:
                modify_args['VolumeType'] = target_type
                changes_requested = True
                # 타입 변경 시 필요한 IOPS/Throughput 설정
                if target_type in ['io1', 'io2']:
                    modify_args['Iops'] = iops if iops is not None else (current_iops if current_iops is not None else 100)
                elif target_type == 'gp3':
                    modify_args['Iops'] = iops if iops is not None else (current_iops if current_iops is not None else 3000)
                    modify_args['Throughput'] = throughput if throughput is not None else (current_throughput if current_throughput is not None else 125)

            # 크기 변경 처리
            if target_size and target_size != current_size:
                if target_size < current_size:
                    msg = f"볼륨 {volume_id} 크기 축소({current_size}GB -> {target_size}GB)는 지원되지 않습니다."
                    logger.error(msg)
                    return {'success': False, 'error': msg}
                modify_args['Size'] = target_size
                changes_requested = True

            # 변경할 내용이 없으면 성공 처리
            if not changes_requested:
                logger.info(f"볼륨 {volume_id}에 대해 요청된 변경 사항이 없습니다.")
                return {'success': True, 'message': 'No changes requested.'}

            # 변경 요청 실행
            response = self.ec2_client.modify_volume(**modify_args)
            modification = response.get('VolumeModification', {})
            logger.info(f"볼륨 속성 변경 요청 완료. Modification details: {modification}")

            return {
                'success': True,
                'message': f"볼륨 속성 변경 요청 성공.",
                'modification_details': modification
            }

        except ClientError as e:
            logger.error(f"볼륨 속성 변경 중 오류 발생: {str(e)}")
            return {'success': False, 'error': str(e)}

    def _get_volume_info(self, volume_id):
        """
        단일 볼륨 정보 조회 (내부 헬퍼 함수)
        """
        try:
            response = self.ec2_client.describe_volumes(VolumeIds=[volume_id])
            if response['Volumes']:
                volume = response['Volumes'][0]
                return {
                    'volume_id': volume['VolumeId'],
                    'volume_type': volume['VolumeType'],
                    'size': volume['Size'],
                    'state': volume.get('State'),
                    'iops': volume.get('Iops'),
                    'throughput': volume.get('Throughput'),
                    'attachments': volume.get('Attachments', [])
                }
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidVolume.NotFound':
                 logger.warning(f"볼륨 {volume_id} 정보를 찾을 수 없습니다 (아마도 삭제됨).")
            else:
                 logger.error(f"볼륨 {volume_id} 정보 조회 중 오류: {str(e)}")
        return None

    # 스냅샷 및 볼륨 상태 확인 메서드는 액션 실행 흐름에 따라
    # RecommendationExecutor 클래스에서 관리하는 것이 더 적합할 수 있음
    # def check_snapshot_status(self, snapshot_id):
    #     ...
    # def check_volume_status(self, volume_id):
    #     ...
    # def _is_volume_safe_to_detach(self, volume_id, instance_id):
    # # 루트 볼륨 여부 등 확인 로직
    #     ...
