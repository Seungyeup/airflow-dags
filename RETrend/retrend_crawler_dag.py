import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# KST 타임존
local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retrend_real_estate_crawler",
    default_args=default_args,
    start_date=local_tz.datetime(2023, 1, 1, 0, 0),  # 반드시 과거 시점
    schedule_interval="0 0 * * *",                   # Airflow 2에서는 schedule_interval 사용
    catchup=False,
    params={
        "smoke": False,
        "pyeonginfo_limit_complexes": 0,
        "pyeonginfo_sleep": 0.5,
        "trade_limit_complexes": 0,
        "trade_limit_areas": 0,
        "trade_limit_pages": 0,
        "trade_sleep": 0.5,
        # KREB(국토부) 경로용 파라미터
        "kreb_use": False,
        "kreb_lawd_codes": "",  # 콤마구분, 또는 S3 파일 사용
        "kreb_lawd_codes_s3": "",
        "kreb_start_ym": "",
        "kreb_end_ym": "",
        "kreb_num_rows": 100,
        "kreb_limit_codes": 0,
        "kreb_limit_months": 0,
        "kreb_limit_pages": 0,
        "kreb_sleep": 0.5,
    },
    tags=["retrend", "crawler", "minio"],
) as dag:

    start_task = KubernetesPodOperator(
        task_id="start_crawler_pipeline",
        name="start-crawler-pipeline",
        namespace="airflow",  # Airflow 설치 네임스페이스
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["bash", "-cx"],
        arguments=["echo 'Starting real estate crawling pipeline'"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_shido = KubernetesPodOperator(
        task_id="extract_shido_data",
        name="extract-shido-data",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["python"],
        arguments=["/app/src/extract_shido_to_excel.py"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_shigungu = KubernetesPodOperator(
        task_id="extract_shigungu_data",
        name="extract-shigungu-data",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["python"],
        arguments=["/app/src/extract_shigungu_to_csv.py"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_eupmeandong = KubernetesPodOperator(
        task_id="extract_eupmeandong_data",
        name="extract-eupmeandong-data",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["python"],
        arguments=["/app/src/extract_eupmeandong_to_csv.py"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_complexes = KubernetesPodOperator(
        task_id="extract_complexes_data",
        name="extract-complexes-data",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["python"],
        arguments=["/app/src/extract_complexes_to_csv.py"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_pyeonginfo = KubernetesPodOperator(
        task_id="extract_pyeonginfo_data",
        name="extract-pyeonginfo-data",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["python"],
        arguments=["/app/src/extract_pyeonginfo_to_csv_s3.py"],
        env_vars={
            "DRY_RUN_SAMPLE": "{{ '1' if params.smoke else '0' }}",
            "NO_WRITE": "{{ '1' if params.smoke else '0' }}",
            "PYEONGINFO_LIMIT_COMPLEXES": "{{ params.pyeonginfo_limit_complexes }}",
            "PYEONGINFO_SLEEP": "{{ params.pyeonginfo_sleep }}"
        },
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_trade_history = KubernetesPodOperator(
        task_id="extract_trade_history_data",
        name="extract-trade-history-data",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["python"],
        arguments=["/app/src/extract_trade_history_s3.py"],
        env_vars={
            "DRY_RUN_SAMPLE": "{{ '1' if params.smoke else '0' }}",
            "NO_WRITE": "{{ '1' if params.smoke else '0' }}",
            "TRADE_LIMIT_COMPLEXES": "{{ params.trade_limit_complexes }}",
            "TRADE_LIMIT_AREAS": "{{ params.trade_limit_areas }}",
            "TRADE_LIMIT_PAGES": "{{ params.trade_limit_pages }}",
            "TRADE_SLEEP": "{{ params.trade_sleep }}"
        },
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    # 한국 부동산원(국토부 실거래가) – 대안 수집 경로
    kreb_apt_trade = KubernetesPodOperator(
        task_id="kreb_apt_trade_bronze",
        name="kreb-apt-trade-bronze",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["python"],
        arguments=["/app/src/kreb/extract_apt_trade_to_csv_s3.py"],
        env_vars={
            "KREB_SERVICE_KEY": "{{ var.value.KREB_SERVICE_KEY }}",
            "KREB_LAWD_CODES": "{{ params.kreb_lawd_codes }}",
            "KREB_LAWD_CODES_S3": "{{ params.kreb_lawd_codes_s3 }}",
            "START_YM": "{{ params.kreb_start_ym }}",
            "END_YM": "{{ params.kreb_end_ym }}",
            "KREB_NUM_ROWS": "{{ params.kreb_num_rows }}",
            "KREB_LIMIT_CODES": "{{ params.kreb_limit_codes }}",
            "KREB_LIMIT_MONTHS": "{{ params.kreb_limit_months }}",
            "KREB_LIMIT_PAGES": "{{ params.kreb_limit_pages }}",
            "KREB_SLEEP": "{{ params.kreb_sleep }}",
            # 스모크 지원(미리보기/비쓰기)
            "DRY_RUN_SAMPLE": "{{ '1' if params.smoke else '0' }}",
            "NO_WRITE": "{{ '1' if params.smoke else '0' }}",
        },
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    end_task = KubernetesPodOperator(
        task_id="end_crawler_pipeline",
        name="end-crawler-pipeline",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["bash", "-cx"],
        arguments=["echo 'Real estate crawling pipeline finished'"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )


    start_task \
        >> extract_shido \
        >> extract_shigungu \
        >> extract_eupmeandong \
        >> extract_complexes \
        >> kreb_apt_trade \
        >> end_task

    # start_task \
    #     >> extract_shido \
    #     >> extract_shigungu \
    #     >> extract_eupmeandong \
    #     >> extract_complexes \
    #     # >> extract_pyeonginfo \
    #     # >> extract_trade_history \
    #     >> kreb_apt_trade_bronze \
    #     >> end_task
