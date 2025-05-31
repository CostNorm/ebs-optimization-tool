import boto3
import logging
from datetime import datetime, timedelta

# Lambda 환경에 맞게 import 경로 수정
from idle_detector import IdleVolumeDetector # 주석 처리 -> 주석 해제
from overprovisioned_detector import OverprovisionedVolumeDetector # 주석 처리 -> 주석 해제
from config import EBS_IDLE_VOLUME_CRITERIA as IDLE_VOLUME_CRITERIA, \
                    EBS_OVERPROVISIONED_CRITERIA as OVERPROVISIONED_CRITERIA, \
                    EBS_METRIC_PERIOD as METRIC_PERIOD
from utils import calculate_monthly_cost, get_tags_as_dict

logger = logging.getLogger()
# Lambda 환경에서는 기본 로거 설정이 다를 수 있으므로, 필요 시 핸들러 추가 고려
# logger.setLevel(logging.INFO)

class EBSAnalyzer:
    """
    EBS 볼륨 분석기 - 유휴 상태와 과대 프로비저닝된 볼륨을 식별
    """

    def __init__(self, region):
        """
        :param region: 분석할 AWS 리전
        """
        self.region = region
        self.ec2_client = boto3.client('ec2', region_name=region)
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=region)

        # 감지기 초기화
        self.idle_detector = IdleVolumeDetector(
            region,
            self.ec2_client,
            self.cloudwatch_client,
            IDLE_VOLUME_CRITERIA
        )

        self.overprovisioned_detector = OverprovisionedVolumeDetector(
            region,
            self.ec2_client,
            self.cloudwatch_client,
            OVERPROVISIONED_CRITERIA
        )

    def get_all_ebs_volumes(self):
        """
        모든 EBS 볼륨 정보를 수집

        :return: 볼륨 정보 리스트
        """
        volumes = []
        paginator = self.ec2_client.get_paginator('describe_volumes')
        page_iterator = paginator.paginate()

        for page in page_iterator:
            volumes.extend(page['Volumes'])

        logger.info(f"{self.region} 리전에서 {len(volumes)}개 EBS 볼륨을 발견했습니다.")
        return volumes

    def get_volume_metrics(self, volume_id, volume_type):
        """
        볼륨의 CloudWatch 메트릭 데이터를 수집

        :param volume_id: EBS 볼륨 ID
        :param volume_type: 볼륨 유형
        :return: 수집된 메트릭 데이터
        """
        end_time = datetime.now()
        # days_to_check 설정값을 config에서 가져오도록 수정
        days_to_check = IDLE_VOLUME_CRITERIA.get('days_to_check', 14) # 기본값 14일
        start_time = end_time - timedelta(days=days_to_check)

        # 수집할 기본 메트릭 목록
        metric_names = [
            'VolumeIdleTime',
            'VolumeReadOps',
            'VolumeWriteOps',
            'VolumeReadBytes',
            'VolumeWriteBytes',
            'VolumeTotalReadTime',
            'VolumeTotalWriteTime',
            'VolumeQueueLength'
        ]

        # 볼륨 유형에 따라 BurstBalance 메트릭 추가
        if volume_type in ['gp2', 'st1', 'sc1']:
            metric_names.append('BurstBalance')

        # 메트릭 데이터 수집
        metrics_data = {}

        for metric_name in metric_names:
            try:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace='AWS/EBS',
                    MetricName=metric_name,
                    Dimensions=[{'Name': 'VolumeId', 'Value': volume_id}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=METRIC_PERIOD, # config에서 가져온 값 사용
                    Statistics=['Average', 'Maximum', 'Minimum', 'Sum']
                )

                if response['Datapoints']:
                    # 가장 최근 데이터포인트 찾기 (get_metric_statistics는 정렬 보장 안함)
                    latest_dp = max(response['Datapoints'], key=lambda x: x['Timestamp'])

                    # 전체 기간 통계 계산
                    datapoints = response['Datapoints']
                    count = len(datapoints)
                    avg_value = sum(dp.get('Average', 0) for dp in datapoints) / count if count > 0 else 0
                    max_value = max(dp.get('Maximum', 0) for dp in datapoints) if datapoints else 0
                    min_value = min(dp.get('Minimum', 0) for dp in datapoints) if datapoints else 0

                    # 메트릭 요약 정보 저장
                    metrics_data[metric_name] = {
                        'latest': latest_dp.get('Average'), # 가장 최근 데이터포인트의 Average 값
                        'average': avg_value,
                        'maximum': max_value,
                        'minimum': min_value,
                        'unit': latest_dp.get('Unit'),
                        'datapoints_count': count
                    }
            except Exception as e:
                logger.warning(f"{volume_id} 볼륨의 {metric_name} 메트릭 조회 중 오류 발생: {str(e)}")

        return metrics_data

    def simplify_metrics(self, metrics):
        """
        메트릭 데이터를 간략화 - avg 값만 표시

        :param metrics: 원본 메트릭 데이터
        :return: 간략화된 메트릭 데이터 (avg 값만 포함)
        """
        if not metrics:
            return {}

        simplified = {}

        # 핵심 메트릭만 포함 (분석에 필요한 것들)
        key_metrics = ['VolumeIdleTime', 'VolumeReadOps', 'VolumeWriteOps',
                       'VolumeReadBytes', 'VolumeWriteBytes', 'BurstBalance']

        for metric_name in key_metrics:
            if metric_name in metrics:
                avg_val = metrics[metric_name].get('average', 0)
                simplified[metric_name] = avg_val

                # VolumeIdleTime은 퍼센트로 변환된 값도 추가
                if metric_name == 'VolumeIdleTime':
                    # 기간(초) = METRIC_PERIOD 값 사용 (예: 86400 for daily)
                    # TODO: 정확한 계산을 위해 기간 확인 필요. 일단 60초(1분) 기준으로 계산.
                    idle_percent = (avg_val / 60) * 100 if avg_val is not None else 0
                    simplified[f"{metric_name}_percent"] = round(idle_percent, 2)

        # 다른 메트릭 추가는 필요한 경우 주석 해제
        # if 'VolumeQueueLength' in metrics:
        #     simplified['QueueLength'] = metrics['VolumeQueueLength'].get('average', 0)

        return simplified

    def format_volume_info(self, volume):
        """
        볼륨 정보를 일관된 형식으로 포맷팅

        :param volume: EC2 API에서 반환된 볼륨 정보
        :return: 포맷팅된 볼륨 정보 딕셔너리
        """
        volume_id = volume['VolumeId']
        volume_type = volume['VolumeType']

        # 기본 볼륨 정보
        volume_info = {
            'volume_id': volume_id,
            'volume_type': volume_type,
            'size': volume['Size'],
            'create_time': volume['CreateTime'].isoformat(),
            'state': volume['State'],
            'availability_zone': volume['AvailabilityZone'],
            'encrypted': volume.get('Encrypted', False),
            'iops': volume.get('Iops'), # None일 수 있음
            'throughput': volume.get('Throughput'), # None일 수 있음
            'multi_attach_enabled': volume.get('MultiAttachEnabled', False),
            'monthly_cost': None, # 아래에서 계산
            'attached_instances': [],
            'tags': get_tags_as_dict(volume.get('Tags', [])), # utils 함수 사용
            'name': None # 아래에서 설정
        }

        # 월 비용 계산 (utils 함수 사용)
        volume_info['monthly_cost'] = calculate_monthly_cost(
            volume_info['size'],
            volume_info['volume_type'],
            self.region,
            iops=volume_info['iops'],
            throughput=volume_info['throughput']
        )

        # Name 태그 설정
        volume_info['name'] = volume_info['tags'].get('Name')

        # 연결된 인스턴스 정보 추가
        if volume.get('Attachments'):
            for attachment in volume['Attachments']:
                volume_info['attached_instances'].append({
                    'instance_id': attachment['InstanceId'],
                    'attach_time': attachment['AttachTime'].isoformat(),
                    'device': attachment['Device'],
                    'delete_on_termination': attachment.get('DeleteOnTermination', False),
                    'state': attachment['State']
                })

        # CloudWatch 메트릭 데이터 조회 및 간략화하여 추가
        full_metrics = self.get_volume_metrics(volume_id, volume_type)
        volume_info['metrics'] = self.simplify_metrics(full_metrics)

        return volume_info

    def analyze_volumes(self, volume_ids=None):
        """
        지정된 볼륨 또는 모든 볼륨에 대해 유휴 상태 및 과대 프로비저닝 상태를 분석

        :param volume_ids: 분석할 볼륨 ID 리스트 (None이면 모든 볼륨 분석)
        :return: 분석 결과 딕셔너리
        """
        volumes_to_process = []
        if volume_ids:
            try:
                response = self.ec2_client.describe_volumes(VolumeIds=volume_ids)
                volumes_to_process = response['Volumes']
                logger.info(f"{self.region} 리전에서 지정된 {len(volumes_to_process)}개 볼륨 정보를 조회했습니다.")
            except Exception as e:
                logger.error(f"지정된 볼륨 ID {volume_ids} 조회 중 오류: {e}")
                return {"error": f"Failed to describe specified volumes: {e}"}
        else:
            volumes_to_process = self.get_all_ebs_volumes()

        if not volumes_to_process:
            logger.info(f"{self.region} 리전에서 분석할 볼륨을 찾지 못했습니다.")
            return {
                "summary": {"total_volumes": 0, "idle_count": 0, "overprovisioned_count": 0},
                "results": []
            }

        # 유휴 상태 볼륨 감지
        idle_volumes_details = self.idle_detector.detect_idle_volumes(volumes_to_process)

        # 과대 프로비저닝 볼륨 감지
        # OverprovisionedVolumeDetector의 detect_overprovisioned_volumes는 볼륨 객체 리스트를 인자로 받음
        overprovisioned_volumes_details = self.overprovisioned_detector.detect_overprovisioned_volumes(volumes_to_process)

        # 결과 통합 및 포맷팅
        results = []
        idle_count = 0
        overprovisioned_count = 0
        # disk_usage_status 관련 카운터 추가
        disk_usage_unavailable_count = 0
        # 비용 절감액 합계
        total_estimated_savings = 0

        # 분석된 볼륨 ID를 추적하여 중복 처리 방지 (또는 각 분석 결과에 모든 볼륨 정보 포함)
        analyzed_volume_ids = set()

        # 1. 유휴 볼륨 결과 처리
        for idle_detail in idle_volumes_details:
            volume_id = idle_detail['volume_id']
            analyzed_volume_ids.add(volume_id)
            
            # 기본 볼륨 정보 가져오기 (이미 idle_detail에 포함되어 있을 수 있음)
            # 여기서는 idle_detail의 정보를 우선 사용하고, 필요시 추가 조회
            # find_volume_by_id 함수가 필요할 수 있음 (volumes_to_process에서 찾기)
            volume_obj = next((v for v in volumes_to_process if v['VolumeId'] == volume_id), None)
            if not volume_obj: continue

            formatted_volume = self.format_volume_info(volume_obj) # 공통 포맷팅
            formatted_volume.update({
                'is_idle': True,
                'idle_reason': idle_detail.get('idle_reason', 'N/A'),
                'is_overprovisioned': False, # 유휴 볼륨은 과대 프로비저닝으로 간주 안 함 (정책에 따라 다를 수 있음)
                'overprovisioned_reason': 'N/A',
                'recommendation': idle_detail.get('recommendation', '유휴 상태입니다. 삭제 또는 스냅샷 후 삭제를 고려하세요.'),
                'estimated_monthly_savings': idle_detail.get('estimated_monthly_savings', formatted_volume.get('monthly_cost', 0))
            })
            results.append(formatted_volume)
            idle_count += 1
            total_estimated_savings += formatted_volume.get('estimated_monthly_savings', 0)

        # 2. 과대 프로비저닝 볼륨 결과 처리 (유휴가 아닌 볼륨 대상)
        for op_detail in overprovisioned_volumes_details:
            volume_id = op_detail['volume_id']
            if volume_id in analyzed_volume_ids: # 이미 유휴 볼륨으로 처리된 경우 건너뛰기
                continue
            analyzed_volume_ids.add(volume_id)

            volume_obj = next((v for v in volumes_to_process if v['VolumeId'] == volume_id), None)
            if not volume_obj: continue
            
            formatted_volume = self.format_volume_info(volume_obj)
            
            # disk_usage_status 확인 및 카운트
            if op_detail.get('disk_usage_status') == 'unavailable':
                disk_usage_unavailable_count += 1
                # disk_usage_error_reason을 recommendation에 포함하거나 별도 필드로 제공
                # op_detail에 이미 recommendation이 적절히 설정되어 있을 것으로 기대
            
            formatted_volume.update({
                'is_idle': False,
                'idle_reason': 'N/A',
                'is_overprovisioned': op_detail.get('is_overprovisioned', False), # detector 결과 사용
                'overprovisioned_reason': op_detail.get('overprovisioned_reason', 'N/A'),
                'recommendation': op_detail.get('recommendation', 'N/A'),
                'estimated_monthly_savings': op_detail.get('estimated_monthly_savings', 0),
                # overprovisioned_detector에서 추가된 필드들 반영
                'current_size_gb': op_detail.get('current_size_gb'),
                'recommended_size_gb': op_detail.get('recommended_size_gb'),
                'disk_usage_status': op_detail.get('disk_usage_status'),
                'disk_usage_error_reason': op_detail.get('disk_usage_error_reason'),
                'disk_usage_data': op_detail.get('disk_usage_data'),
                'is_size_overprovisioned': op_detail.get('is_size_overprovisioned'),
                'is_performance_overprovisioned': op_detail.get('is_performance_overprovisioned'),
                'performance_overprovisioned_reason': op_detail.get('performance_overprovisioned_reason')
            })
            results.append(formatted_volume)
            if formatted_volume['is_overprovisioned']:
                overprovisioned_count += 1
                total_estimated_savings += formatted_volume.get('estimated_monthly_savings', 0)

        # 3. 분석되지 않은 나머지 볼륨 처리 (유휴도 아니고, 과대 프로비저닝 분석 대상에도 없었던 볼륨)
        for volume_obj in volumes_to_process:
            if volume_obj['VolumeId'] not in analyzed_volume_ids:
                formatted_volume = self.format_volume_info(volume_obj)
                formatted_volume.update({
                    'is_idle': False,
                    'idle_reason': 'N/A',
                    'is_overprovisioned': False,
                    'overprovisioned_reason': 'N/A',
                    'recommendation': '유휴 또는 과대 프로비저닝 상태가 아닌 것으로 보입니다.',
                    'estimated_monthly_savings': 0,
                    'disk_usage_status': 'not_analyzed' # 또는 'unknown' 등
                })
                results.append(formatted_volume)

        # 최종 요약 정보 생성
        summary = {
            'total_volumes_processed': len(volumes_to_process),
            'idle_detected_count': idle_count,
            'overprovisioned_detected_count': overprovisioned_count,
            'disk_usage_unavailable_count': disk_usage_unavailable_count, # 추가된 카운트
            'total_estimated_monthly_savings': round(total_estimated_savings, 2)
        }
        
        logger.info(f"EBS 볼륨 분석 완료: {summary}")

        return {
            "summary": summary,
            "results": results
        }

    def get_recommendations(self, volume_id):
        # 이 메소드는 현재 구현되지 않았습니다. 필요에 따라 구현할 수 있습니다.
        pass

    # analyze_specific_volume 메소드는 현재 EBSAnalyzer에 없음.
    # 만약 Lambda_function.py 등에서 직접 EBSAnalyzer의 특정 메소드를 호출하여 단일 볼륨을 분석한다면,
    # 해당 로직이 self.idle_detector.is_idle_volume 등을 사용하도록 해야 함.
    # 지금은 analyze_volumes 내에서 volume_ids 파라미터로 단일/다수 볼륨 처리가 가능하므로,
    # analyze_specific_volume을 별도로 두기보다 analyze_volumes를 활용하는 것이 일관성 있을 수 있음.

    # def analyze_specific_volume(self, volume_id):
    #     """ 특정 볼륨 ID에 대한 상세 분석 """
    #     # ... (이 부분은 이전 검색 결과의 costnorm_mcp_server/storage/ebs/analyzer/analyzer.py 참고하여 구성 가능)
    #     # self.idle_detector.is_idle_volume(...) 등을 사용
    #     pass 