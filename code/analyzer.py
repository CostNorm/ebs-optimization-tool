import boto3
import logging
from datetime import datetime, timedelta

# Lambda 환경에 맞게 import 경로 수정
# from idle_detector import IdleVolumeDetector # 주석 처리
# from overprovisioned_detector import OverprovisionedVolumeDetector # 주석 처리
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

        # 감지기 초기화 - 임시 주석 처리
        # self.idle_detector = IdleVolumeDetector(
        #     region,
        #     self.ec2_client,
        #     self.cloudwatch_client,
        #     IDLE_VOLUME_CRITERIA
        # )

        # self.overprovisioned_detector = OverprovisionedVolumeDetector(
        #     region,
        #     self.ec2_client,
        #     self.cloudwatch_client,
        #     OVERPROVISIONED_CRITERIA
        # )

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

        # 유휴 상태 볼륨 감지 - 임시 주석 처리
        # idle_volumes_details = self.idle_detector.detect_idle_volumes(volumes_to_process)
        idle_volumes_details = {} # 임시 빈 딕셔너리 반환

        # 과대 프로비저닝된 볼륨 감지 - 임시 주석 처리
        # overprovisioned_volumes_details = self.overprovisioned_detector.detect_overprovisioned_volumes(volumes_to_process)
        overprovisioned_volumes_details = {} # 임시 빈 딕셔너리 반환

        analysis_results = []
        # 분석 로직은 유지 (감지기 결과 사용 부분 제외)
        for volume in volumes_to_process:
            formatted_info = self.format_volume_info(volume)

            # 감지 결과 통합 (현재는 비어 있음)
            volume_id = formatted_info['volume_id']
            formatted_info['is_idle'] = idle_volumes_details.get(volume_id, {}).get('is_idle', False)
            formatted_info['idle_reason'] = idle_volumes_details.get(volume_id, {}).get('reason')
            formatted_info['is_overprovisioned'] = overprovisioned_volumes_details.get(volume_id, {}).get('is_overprovisioned', False)
            formatted_info['overprovisioned_reason'] = overprovisioned_volumes_details.get(volume_id, {}).get('reason')

            # 추천 액션 결정 로직은 유지 (감지 결과에 따라 달라짐)
            recommendation = 'none'
            if formatted_info['is_idle']:
                recommendation = 'snapshot_and_delete' # 또는 'delete_only'
            elif formatted_info['is_overprovisioned']:
                recommendation = 'modify' # 실제로는 세부 타입 제안 필요

            formatted_info['recommendation'] = recommendation
            analysis_results.append(formatted_info)

        # 요약 정보 계산 (현재는 0으로 표시될 것임)
        idle_count = sum(1 for v in analysis_results if v.get('is_idle'))
        overprovisioned_count = sum(1 for v in analysis_results if v.get('is_overprovisioned'))
        summary = {
            "total_volumes": len(volumes_to_process),
            "idle_count": idle_count,
            "overprovisioned_count": overprovisioned_count
        }

        logger.info(f"분석 완료: 전체 {summary['total_volumes']}, 유휴 {summary['idle_count']}, 과대 {summary['overprovisioned_count']}")

        return {
            "summary": summary,
            "results": analysis_results
        }

    # analyze_specific_volume 메서드는 analyze_volumes(volume_ids=[...])로 대체 가능
    # 필요 시 유지 또는 삭제
    # def analyze_specific_volume(self, volume_id):
    #     ... (analyze_volumes 호출하도록 구현) 