import logging
import os
import json

# Lambda 환경에서는 가격 정보를 외부(e.g., 환경 변수, SSM Parameter Store)에서 가져오는 것이 더 좋음
# 또는 AWS Price List API 사용 고려
from config import EBS_PRICING # config.py에서 가격 정보 가져오기

logger = logging.getLogger()

def calculate_monthly_cost(size_gb, volume_type, region, iops=None, throughput=None):
    """
    EBS 볼륨의 월간 비용을 계산합니다. (gp3, io1, io2 비용 계산 개선)

    :param size_gb: 볼륨 크기 (GB)
    :param volume_type: 볼륨 유형 (gp2, gp3, io1, io2, st1, sc1, standard)
    :param region: AWS 리전
    :param iops: 프로비저닝된 IOPS (gp3, io1, io2 용)
    :param throughput: 프로비저닝된 처리량 (gp3 용, MiBps 단위)
    :return: 월간 비용 (USD) 또는 None (가격 정보 없을 시)
    """
    region_prices = EBS_PRICING.get(region, EBS_PRICING.get('default'))

    if not region_prices:
        logger.warning(f"리전 {region} 또는 기본 리전에 대한 가격 정보를 찾을 수 없습니다.")
        return None

    storage_price = region_prices.get(volume_type, {}).get('storage')

    if storage_price is None:
        logger.warning(f"리전 {region}의 볼륨 타입 {volume_type}에 대한 스토리지 가격을 찾을 수 없습니다. gp2 가격으로 대체합니다.")
        storage_price = region_prices.get('gp2', {}).get('storage', 0.10) # 최종 대체 가격

    # 기본 스토리지 비용
    monthly_cost = size_gb * storage_price

    # --- 타입별 추가 비용 계산 --- 
    if volume_type == 'gp3':
        # gp3 IOPS 비용 (기본 3000 IOPS 초과분)
        iops_price = region_prices.get('gp3', {}).get('iops')
        base_iops = 3000
        if iops_price and iops and iops > base_iops:
            monthly_cost += (iops - base_iops) * iops_price
        
        # gp3 Throughput 비용 (기본 125 MiBps 초과분)
        throughput_price = region_prices.get('gp3', {}).get('throughput')
        base_throughput = 125
        if throughput_price and throughput and throughput > base_throughput:
             # 가격은 보통 $/MiBps-월 단위
             monthly_cost += (throughput - base_throughput) * throughput_price

    elif volume_type in ['io1', 'io2']:
        # io1/io2 IOPS 비용
        iops_price = region_prices.get(volume_type, {}).get('iops')
        if iops_price and iops:
            monthly_cost += iops * iops_price
        # io2 Block Express 는 다른 요금 체계 (여기서는 기본 io2 가정)

    return round(monthly_cost, 2) # 소수점 2자리까지 반환

def get_tags_as_dict(tags_list):
    """
    AWS 리소스의 태그 리스트를 딕셔너리로 변환합니다.

    :param tags_list: AWS 리소스의 태그 리스트 [{'Key': 'Name', 'Value': 'value'}, ...]
    :return: 태그 딕셔너리 {'Name': 'value', ...}
    """
    if not tags_list:
        return {}

    return {tag['Key']: tag['Value'] for tag in tags_list}

def format_bytes(size_bytes):
    """
    바이트 값을 사람이 읽기 쉬운 형식으로 변환합니다.

    :param size_bytes: 바이트 단위의 크기
    :return: 변환된 문자열 (B, KB, MB, GB, TB)
    """
    if size_bytes is None or size_bytes < 0:
        return "N/A"

    power = 1024 # 2**10 아님
    n = 0
    power_labels = {0: 'B', 1: 'KiB', 2: 'MiB', 3: 'GiB', 4: 'TiB'} # IEC 표준 사용

    while size_bytes >= power and n < len(power_labels) - 1:
        size_bytes /= power
        n += 1

    return f"{size_bytes:.2f} {power_labels[n]}" 