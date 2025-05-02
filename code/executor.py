import logging
import json
import time
import boto3
from datetime import datetime

# Lambda 환경에 맞게 import 경로 수정
from actions import EBSActionExecutor

logger = logging.getLogger()
# Lambda 환경에서는 기본 로거 설정이 다를 수 있으므로, 필요 시 핸들러 추가 고려
# logger.setLevel(logging.INFO)

class RecommendationExecutor:
    """
    분석 결과의 권장 조치를 실행하는 클래스
    """

    def __init__(self, region):
        """
        :param region: AWS 리전
        """
        self.region = region
        self.ebs_action_executor = EBSActionExecutor(region)
        self.execution_history = [] # Lambda에서는 상태 유지가 어려우므로, 이력 관리는 외부(e.g., DynamoDB) 고려
        self.ec2_client = boto3.client('ec2', region_name=region)

    def execute_recommendation(self, volume_info, action_type):
        """
        볼륨 유형(유휴/과대)에 관계없이 권장 조치를 실행합니다.
        이 함수는 Lambda 핸들러에서 호출될 메인 실행 함수 역할을 합니다.

        :param volume_info: 분석 결과에서 가져온 볼륨 정보 딕셔너리 (필요한 키 포함)
        :param action_type: 실행할 조치 유형
        :return: 결과 딕셔너리
        """
        volume_id = volume_info['volume_id']
        result = {
            'volume_id': volume_id,
            'action_type': action_type,
            'success': False,
            'timestamp': datetime.now().isoformat(),
            'details': {},
            'status': 'initiated'
        }

        # --- 유효성 검사 및 사전 준비 --- 
        if not all([volume_id, action_type]):
            result['details']['error'] = "volume_id와 action_type은 필수입니다."
            return result

        # 볼륨 상세 정보 가져오기 (액션 실행 전 최신 상태 확인)
        live_volume_info = self.ebs_action_executor._get_volume_info(volume_id)
        if not live_volume_info:
            result['details']['error'] = f"볼륨 {volume_id} 정보를 찾을 수 없습니다."
            result['status'] = 'failed'
            return result
        # 분석 시점 정보와 실시간 정보를 결합 (필요 시)
        volume_info.update(live_volume_info) # 실시간 상태, iops, throughput 등 업데이트

        # --- 루트 볼륨 보호 로직 ---
        is_root = False
        root_device_check_needed = action_type in ['snapshot_and_delete', 'resize', 'change_type_and_resize']

        if root_device_check_needed and volume_info.get('attachments'):
            for attachment in volume_info['attachments']:
                instance_id = attachment.get('InstanceId')
                device_name = attachment.get('Device')
                if instance_id and device_name:
                    if self._is_root_volume(instance_id, device_name):
                        is_root = True
                        break

        # 루트 볼륨 대상 위험 작업 방지
        if is_root:
            if action_type == 'snapshot_and_delete':
                 warning_msg = f"작업 건너뜀: 볼륨 {volume_id}은(는) 루트 볼륨이므로 삭제할 수 없습니다."
                 logger.warning(warning_msg)
                 result['status'] = 'skipped_root_volume'
                 result['details']['error'] = warning_msg
                 return result
            elif action_type in ['resize', 'change_type_and_resize']:
                target_size = volume_info.get('recommended_size') # 분석 결과의 권장 크기
                current_size = volume_info.get('size')
                # 권장 크기가 있고 현재 크기보다 작으면 축소 시도 -> 방지
                if target_size is not None and target_size < current_size:
                    warning_msg = f"작업 건너뜀: 볼륨 {volume_id}은(는) 루트 볼륨이므로 크기 축소({current_size}GB -> {target_size}GB)를 할 수 없습니다."
                    logger.warning(warning_msg)
                    result['status'] = 'skipped_root_volume_resize'
                    result['details']['error'] = warning_msg
                    return result
            elif action_type == 'change_type':
                 logger.warning(f"주의: 루트 볼륨 {volume_id}의 타입 변경({action_type})을 진행합니다.")
        # --- 루트 볼륨 보호 로직 끝 ---

        logger.info(f"볼륨 {volume_id}에 대한 '{action_type}' 작업 시작 중...")

        try:
            # --- 액션 실행 분기 --- 
            action_func_map = {
                'snapshot_only': self._execute_snapshot_only,
                'snapshot_and_delete': self._execute_snapshot_and_delete,
                'change_type': self._execute_change_type,
                'resize': self._execute_resize, # 과대 프로비저닝용
                'change_type_and_resize': self._execute_change_type_and_resize
            }

            if action_type in action_func_map:
                action_result = action_func_map[action_type](volume_info, result)
                # 결과 업데이트 (action_result는 result 딕셔너리를 직접 수정)
            else:
                result['details']['error'] = f"지원되지 않는 작업 유형: {action_type}"
                result['success'] = False
                result['status'] = 'failed'

        except Exception as e:
            logger.error(f"권장 조치 실행 중 예외 발생 (볼륨: {volume_id}, 액션: {action_type}): {str(e)}", exc_info=True)
            result['success'] = False
            result['status'] = 'error'
            result['details']['error'] = f"Unexpected error: {str(e)}"

        # Lambda에서는 실행 기록 저장을 외부로 위임
        # self.execution_history.append(result)
        logger.info(f"볼륨 {volume_id} 작업 '{action_type}' 결과: Success={result['success']}, Status={result.get('status')}")
        return result

    # --- 개별 액션 실행 함수 --- 

    def _execute_snapshot_only(self, volume_info, result):
        volume_id = volume_info['volume_id']
        tags = self._generate_snapshot_tags(volume_info, 'snapshot_only')
        snapshot_id = self.ebs_action_executor.create_snapshot(
            volume_id,
            description=f"Snapshot before potential action - {datetime.now().strftime('%Y-%m-%d')}",
            tags=tags
        )
        if snapshot_id:
            result['details']['snapshot_id'] = snapshot_id
            result['details']['action'] = "스냅샷 생성 요청 완료"
            result['details']['note'] = "스냅샷 생성은 백그라운드에서 계속 진행됩니다."
            result['success'] = True
            result['status'] = 'snapshot_initiated'
        else:
            result['details']['error'] = "스냅샷 생성 요청 실패"
            result['status'] = 'failed'
        return result

    def _execute_snapshot_and_delete(self, volume_info, result):
        volume_id = volume_info['volume_id']
        # 1. 스냅샷 생성
        tags = self._generate_snapshot_tags(volume_info, 'snapshot_and_delete')
        snapshot_id = self.ebs_action_executor.create_snapshot(
            volume_id,
            description=f"Snapshot before deletion - {datetime.now().strftime('%Y-%m-%d')}",
            tags=tags
        )
        if not snapshot_id:
            result['details']['error'] = "선행 작업 실패: 스냅샷 생성 요청 실패"
            result['status'] = 'failed'
            return result
        result['details']['snapshot_id'] = snapshot_id
        logger.info(f"볼륨 {volume_id} 삭제 전 스냅샷 {snapshot_id} 생성 요청 완료.")

        # 2. 볼륨 분리 (필요 시)
        if volume_info.get('attachments'):
            logger.info(f"볼륨 {volume_id} 분리 시도 중...")
            detach_success = self.ebs_action_executor.detach_volume(volume_id)
            if not detach_success:
                # 분리 실패 시 삭제 진행 불가 (Idempotency는 detach_volume에서 처리)
                result['details']['error'] = f"볼륨 {volume_id} 분리 요청 실패. 삭제를 진행할 수 없습니다."
                result['status'] = 'failed'
                return result
            logger.info(f"볼륨 {volume_id} 분리 요청 완료. 분리 완료까지 시간 소요될 수 있음.")
            # 분리 완료를 기다릴 필요 없이 삭제 진행 (API가 상태 체크)

        # 3. 볼륨 삭제
        logger.info(f"볼륨 {volume_id} 삭제 시도 중...")
        delete_result = self.ebs_action_executor.delete_volume(volume_id)
        if delete_result: # delete_volume은 boolean 반환
            result['details']['action'] = "스냅샷 생성 및 볼륨 삭제 요청 완료"
            result['details']['note'] = "작업은 백그라운드에서 계속 진행됩니다."
            result['success'] = True
            result['status'] = 'delete_initiated'
        else:
            result['details']['error'] = f"볼륨 {volume_id} 삭제 요청 실패"
            result['status'] = 'failed'
        return result

    def _execute_change_type(self, volume_info, result):
        volume_id = volume_info['volume_id']
        current_type = volume_info.get('volume_type')
        # 권장 타입 정보가 volume_info에 포함되어 있다고 가정
        target_type = volume_info.get('recommended_type', self._determine_target_volume_type(current_type))

        if current_type == target_type:
            result['details']['message'] = f"볼륨 유형 {current_type}에서 변경이 필요하지 않습니다."
            result['success'] = True
            result['status'] = 'no_change_needed'
            return result

        logger.info(f"볼륨 {volume_id} 타입 변경 시도: {current_type} -> {target_type}")
        # modify_volume 사용
        modify_result = self.ebs_action_executor.modify_volume(
            volume_id,
            target_type=target_type,
            iops=volume_info.get('iops'), # 현재 값 또는 권장값 전달
            throughput=volume_info.get('throughput') # 현재 값 또는 권장값 전달
        )

        result['details'].update(modify_result)
        result['success'] = modify_result.get('success', False)
        if result['success']:
             result['details']['action'] = f"볼륨 타입을 {current_type}에서 {target_type}(으)로 변경 요청 완료"
             result['details']['note'] = "변경 작업은 백그라운드에서 진행됩니다."
             result['status'] = 'modification_initiated'
        else:
             result['details']['error'] = modify_result.get('error', "타입 변경 요청 실패")
             result['status'] = 'failed'
        return result

    def _execute_resize(self, volume_info, result):
        volume_id = volume_info['volume_id']
        current_size = volume_info.get('size')
        # 권장 크기 정보가 volume_info에 포함되어 있다고 가정
        target_size = volume_info.get('recommended_size')

        if target_size is None:
             result['details']['error'] = "권장 크기 정보가 없어 크기 조정을 실행할 수 없습니다."
             result['status'] = 'failed'
             return result

        if target_size >= current_size:
            result['details']['message'] = f"볼륨 크기 {current_size}GB에서 변경(증가/유지)이 필요하지 않거나 권장되지 않습니다."
            result['success'] = True
            result['status'] = 'no_change_needed'
            return result

        # 크기 축소는 modify_volume에서 방지됨, 여기서는 로깅만
        logger.info(f"볼륨 {volume_id} 크기 변경(축소) 시도: {current_size}GB -> {target_size}GB")

        # modify_volume 사용 (modify_volume 내부에서 축소 방지)
        modify_result = self.ebs_action_executor.modify_volume(
            volume_id,
            target_size=target_size
        )

        result['details'].update(modify_result)
        result['success'] = modify_result.get('success', False)
        if result['success']:
             result['details']['action'] = f"볼륨 크기를 {current_size}GB에서 {target_size}GB(으)로 변경 요청 완료"
             result['details']['note'] = "변경 작업은 백그라운드에서 진행됩니다."
             result['status'] = 'modification_initiated'
        else:
             result['details']['error'] = modify_result.get('error', "크기 변경 요청 실패")
             result['status'] = 'failed'
        return result

    def _execute_change_type_and_resize(self, volume_info, result):
        volume_id = volume_info['volume_id']
        current_type = volume_info.get('volume_type')
        current_size = volume_info.get('size')
        target_type = volume_info.get('recommended_type', self._determine_target_volume_type(current_type))
        target_size = volume_info.get('recommended_size')

        if target_size is None:
             result['details']['error'] = "권장 크기 정보가 없어 크기 조정을 포함한 변경을 실행할 수 없습니다."
             result['status'] = 'failed'
             return result

        # 변경 필요 여부 확인
        type_changed = target_type != current_type
        size_changed = target_size != current_size

        if not type_changed and not size_changed:
            result['details']['message'] = f"볼륨 타입({current_type}) 및 크기({current_size}GB) 변경이 필요하지 않습니다."
            result['success'] = True
            result['status'] = 'no_change_needed'
            return result

        # 크기 축소 방지 (modify_volume에서 처리하지만 여기서도 로깅)
        if target_size < current_size:
             logger.warning(f"볼륨 {volume_id} 크기 축소({current_size}GB -> {target_size}GB)는 지원되지 않습니다.")
             # modify_volume 호출 시 에러 처리됨

        logger.info(f"볼륨 {volume_id} 타입 및 크기 변경 시도: {current_type}->{target_type}, {current_size}GB->{target_size}GB")

        modify_result = self.ebs_action_executor.modify_volume(
            volume_id,
            target_type=target_type if type_changed else None,
            target_size=target_size if size_changed else None,
            iops=volume_info.get('iops'),
            throughput=volume_info.get('throughput')
        )

        result['details'].update(modify_result)
        result['success'] = modify_result.get('success', False)
        if result['success']:
             result['details']['action'] = f"볼륨 타입 및 크기 변경 요청 완료 ({current_type}->{target_type}, {current_size}GB->{target_size}GB)"
             result['details']['note'] = "변경 작업은 백그라운드에서 진행됩니다."
             result['status'] = 'modification_initiated'
        else:
             result['details']['error'] = modify_result.get('error', "타입/크기 변경 요청 실패")
             result['status'] = 'failed'
        return result

    # --- Helper Functions --- 

    def _generate_snapshot_tags(self, volume_info, action_type):
        tags = {
            'Name': f"AutoSnapshot-{volume_info['volume_id']}-{action_type[:10]}", # 이름 길이 제한 고려
            'AutoCreated': 'true',
            'Source': 'Lambda-EBS-Optimizer',
            'TriggeringAction': action_type,
            'CreationTimestamp': datetime.now().isoformat()
        }
        if volume_info.get('name'):
            tags['VolumeName'] = volume_info['name'] # 원본 볼륨 이름
        # 기존 볼륨 태그 일부 복사 (선택 사항)
        # if volume_info.get('tags'):
        #     for k, v in volume_info['tags'].items():
        #         if k.lower() not in ['name', 'aws: ']: # 일부 시스템 태그 제외
        #              tags[f"VolumeTag_{k}"] = v
        return tags

    def _is_root_volume(self, instance_id, device_name):
        """
        주어진 디바이스가 인스턴스의 루트 볼륨인지 확인
        """
        try:
            response = self.ec2_client.describe_instances(InstanceIds=[instance_id])
            if not response['Reservations'] or not response['Reservations'][0]['Instances']:
                logger.warning(f"루트 볼륨 확인 중 인스턴스 {instance_id}를 찾을 수 없습니다.")
                return False

            instance = response['Reservations'][0]['Instances'][0]
            root_device = instance.get('RootDeviceName')

            if root_device and root_device == device_name:
                logger.info(f"확인: {device_name}은(는) 인스턴스 {instance_id}의 루트 디바이스입니다.")
                return True
            return False

        except ClientError as e:
            # 접근 권한 문제 등
            logger.error(f"루트 볼륨 확인 중 오류 발생 (인스턴스: {instance_id}): {str(e)}")
            return False # 오류 시 안전하게 루트가 아니라고 가정하지 않음 (오히려 루트일 가능성)
        except Exception as e:
            logger.error(f"루트 볼륨 확인 중 예외 발생 (인스턴스: {instance_id}): {str(e)}")
            return False

    def _determine_target_volume_type(self, current_type):
        """
        현재 볼륨 타입에 따라 변경할 목표 타입 결정 (단순 예시)
        실제로는 분석 결과 (volume_info['recommended_type'])를 사용해야 함
        """
        logger.warning("_determine_target_volume_type 호출됨 - 분석 결과의 recommended_type 사용 권장")
        if current_type in ['io1', 'io2', 'gp2', 'st1', 'sc1', 'standard']:
            return 'gp3' # 일반적인 최적화 방향
        else:
            return current_type

    # _calculate_recommended_size 는 Analyzer에서 계산된 값을 사용해야 하므로 여기서는 불필요
    # def _calculate_recommended_size(self, volume_info):
    #     ...

    # Lambda 환경에서는 상태 저장/로드가 어려움
    # def get_execution_history(self):
    #     return self.execution_history
    # def save_execution_history(self, filepath):
    #     ...

    # 상태 확인 함수들은 필요 시 여기에 둘 수 있으나, 지금은 사용되지 않음
    # def _get_volume_info(self, volume_id):
    #     return self.ebs_action_executor._get_volume_info(volume_id)
    # def check_snapshot_status(self, snapshot_id):
    #     ...
    # def check_volume_status(self, volume_id):
    #     ...
    # def check_volume_modification_status(self, volume_id):
    #     ... 