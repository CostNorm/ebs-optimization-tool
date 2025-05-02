import json
import logging
import os

# 필요한 모듈 임포트
from analyzer import EBSAnalyzer
from executor import RecommendationExecutor

# 로거 설정 (Lambda 환경에 맞게 기본 설정 사용)
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO').upper())

def lambda_handler(event, context):
    """
    AWS Lambda 함수 핸들러.
    API Gateway 또는 직접 호출을 통해 트리거됩니다.

    event (dict): 입력 이벤트 객체. 다음 키를 포함해야 함:
        - operation (str): 수행할 작업 ('analyze' 또는 'execute').
        - region (str): 대상 AWS 리전.
        - (선택) volume_id (str): 특정 볼륨 ID (analyze 및 execute에 사용).
        - (선택) volume_ids (list[str]): 분석할 볼륨 ID 목록 (analyze에 사용).
        - (선택) action_type (str): 실행할 액션 유형 (execute에 필요).
        - (선택) volume_info (dict): 액션 실행에 필요한 볼륨 정보 (execute에 필요).
    """
    logger.info(f"Received event: {json.dumps(event)}")

    # 필수 파라미터 확인
    operation = event.get('operation')
    region = event.get('region')

    if not operation or not region:
        logger.error("필수 파라미터 누락: 'operation'과 'region'이 필요합니다.")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': "Missing required parameters: operation, region"})
        }

    response_body = {}
    status_code = 200

    try:
        if operation == 'analyze':
            logger.info(f"{region} 리전 EBS 볼륨 분석 시작...")
            analyzer = EBSAnalyzer(region=region)
            volume_ids = event.get('volume_ids') # 리스트 형태
            volume_id = event.get('volume_id') # 단일 ID
            
            # 단일 ID가 주어지면 리스트로 변환
            if volume_id and not volume_ids:
                 volume_ids = [volume_id]
                 
            analysis_result = analyzer.analyze_volumes(volume_ids=volume_ids)
            response_body = analysis_result
            logger.info(f"분석 완료. 결과: {analysis_result.get('summary')}")

        elif operation == 'execute':
            logger.info(f"{region} 리전 EBS 볼륨 액션 실행 시작...")
            volume_id = event.get('volume_id')
            action_type = event.get('action_type')
            volume_info = event.get('volume_info') # 액션 실행에 필요한 상세 정보

            if not volume_id or not action_type:
                logger.error("액션 실행 필수 파라미터 누락: 'volume_id', 'action_type'이 필요합니다.")
                status_code = 400
                response_body = {'error': "Missing required parameters for execute: volume_id, action_type"}
            elif not volume_info: # volume_info가 없으면 기본 정보라도 생성
                logger.warning("'volume_info' 파라미터가 없습니다. 기본 정보로 실행합니다.")
                volume_info = {'volume_id': volume_id}

            if status_code == 200:
                executor = RecommendationExecutor(region=region)
                # volume_info에 region 정보가 없을 수 있으므로 추가
                volume_info['region'] = region
                execution_result = executor.execute_recommendation(volume_info, action_type)
                response_body = execution_result
                logger.info(f"액션 실행 완료. 결과: {execution_result}")

        else:
            logger.error(f"지원되지 않는 작업 유형: {operation}")
            status_code = 400
            response_body = {'error': f"Unsupported operation: {operation}"}

    except Exception as e:
        logger.error(f"처리 중 예외 발생: {str(e)}", exc_info=True)
        status_code = 500
        response_body = {'error': f"Internal server error: {str(e)}"}

    # Lambda Proxy 통합 응답 형식
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps(response_body)
    } 