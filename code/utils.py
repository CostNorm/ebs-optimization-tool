import logging
import os
import json

# Lambda 환경에서는 가격 정보를 외부(e.g., 환경 변수, SSM Parameter Store)에서 가져오는 것이 더 좋음
# 또는 AWS Price List API 사용 고려
from config import EBS_PRICING # config.py에서 가격 정보 가져오기

logger = logging.getLogger()

def calculate_monthly_cost(size_gb, volume_type, region_name, iops=None, throughput=None):
    """
    Calculates the estimated monthly cost of an EBS volume.
    """
    # Ensure iops and throughput are numbers if provided
    current_iops = None
    if iops is not None:
        try:
            current_iops = int(iops)
        except ValueError:
            logger.error(f"Invalid value for IOPS: {iops}. Must be a number.")
            # Decide how to handle: return error, use default, or ignore for cost calculation
            # For now, let's assume it might affect cost calculation if not default.
            pass # Or set to a default that doesn't add cost, e.g., 0 or base_iops for gp3

    current_throughput = None
    if throughput is not None:
        try:
            current_throughput = int(throughput) # or float if decimal values are possible/expected
        except ValueError:
            logger.error(f"Invalid value for Throughput: {throughput}. Must be a number.")
            # Similar handling as IOPS
            pass

    try:
        pricing_info = EBS_PRICING.get(region_name, EBS_PRICING['default'])
        type_pricing = pricing_info.get(volume_type)

        if not type_pricing:
            logger.warning(f"Pricing for volume type '{volume_type}' in region '{region_name}' not found. Using default gp2 pricing.")
            type_pricing = EBS_PRICING['default']['gp2']
            # return 0.0 # Or handle as an error / use a default

        monthly_cost = float(size_gb) * type_pricing['storage']

        if volume_type == 'gp3':
            gp3_pricing = type_pricing
            # Default provisioned values for gp3 if not specified (or if free tier)
            # AWS provides 3,000 IOPS and 125 MiBps free with gp3 storage
            base_iops = 3000 
            base_throughput = 125  # MiBps

            # Calculate cost for IOPS provisioned above the free tier
            if current_iops is not None and gp3_pricing.get('iops') and current_iops > base_iops:
                monthly_cost += (current_iops - base_iops) * gp3_pricing['iops']
            
            # Calculate cost for throughput provisioned above the free tier
            if current_throughput is not None and gp3_pricing.get('throughput') and current_throughput > base_throughput:
                monthly_cost += (current_throughput - base_throughput) * gp3_pricing['throughput']
        
        elif volume_type in ['io1', 'io2']:
            # io1 and io2 have provisioned IOPS costs
            if current_iops is not None and type_pricing.get('iops'):
                monthly_cost += current_iops * type_pricing['iops']
        
        # For other volume types like gp2, st1, sc1, standard, cost is mainly based on storage.
        # (Additional logic for specific features of those types could be added if necessary)

        return round(monthly_cost, 2)

    except Exception as e:
        logger.error(f"Error calculating monthly cost for size {size_gb}GB, type {volume_type}, region {region_name}: {str(e)}", exc_info=True)
        return 0.0  # Return 0 or raise error on failure

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