# EBS Optimization Tool

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Infrastructure: Terraform](https://img.shields.io/badge/Infrastructure-Terraform-blueviolet)](https://www.terraform.io/)
[![Language: Python](https://img.shields.io/badge/Language-Python-3776AB.svg)](https://www.python.org/)

The **EBS Optimization Tool** is a serverless application that automatically analyzes Elastic Block Store (EBS) volumes in your AWS environment to identify cost and performance optimization opportunities and execute recommended actions.

This tool aims to reduce costs and improve resource efficiency by detecting idle and over-provisioned volumes. It is designed to automate the entire process, from analysis to remediation.

## Key Features

- **Idle Volume Detection**:

  - Immediately detects volumes in an `available` state (not attached to any instance).
  - Analyzes the `VolumeIdleTime` metric from CloudWatch to identify `in-use` volumes that have not been accessed for an extended period.

- **Over-provisioned Volume Detection**:

  - **Size Optimization**: Determines if volume size is over-allocated by analyzing actual OS-level disk usage, collected via the CloudWatch Agent or SSM Run Command.
  - **Performance Optimization**: Identifies if performance is over-provisioned by comparing the provisioned IOPS and Throughput of `gp3`, `io1`, and `io2` volumes against their actual usage.

- **Cost Savings Estimation**:

  - Calculates and provides an estimated monthly cost saving for each optimization recommendation.

- **Automated Remediation**:

  - Executes recommended actions based on the analysis results, including:
    - Create a snapshot, then delete the volume (`snapshot_and_delete`)
    - Change volume type (e.g., `gp2` -> `gp3`) (`change_type`)
    - Modify volume size (`resize`)
    - Change volume type and size simultaneously (`change_type_and_resize`)

- **Safety Mechanisms**:
  - **Root Volume Protection**: Automatically prevents dangerous operations, such as deletion or size reduction, on an instance's root volume.
  - **IaC-based Deployment**: Uses Terraform to manage all AWS resources as code, ensuring consistent and reliable deployments.

## Prerequisites

1.  **An AWS Account**
2.  **Terraform (v1.0.0 or higher)**: [Terraform Installation Guide](https://learn.hashicorp.com/tutorials/terraform/install-cli)
3.  **AWS CLI**: With a configured profile. The default profile for this project is `costnorm` (see `IaC/main.tf` and `IaC/variable.tf`).
    ```bash
    aws configure --profile costnorm
    ```
4.  **(Recommended) SSM Agent**: For accurate disk usage analysis, the target EC2 instances should have the [SSM Agent](https://docs.aws.amazon.com/systems-manager/latest/userguide/ssm-agent.html) installed and running. The instance's IAM role also requires the `AmazonSSMManagedInstanceCore` policy.

## Installation and Deployment

Deploy the Lambda function and its required IAM roles and policies using Terraform.

1.  **Clone the repository**:

    ```bash
    git clone <repository_url>
    cd ebs-optimization-tool-main
    ```

2.  **Navigate to the IaC directory**:

    ```bash
    cd IaC
    ```

3.  **(Optional) Modify variables**:
    Open `variable.tf` to customize default values such as the deployment region or function name.

4.  **Initialize Terraform**:

    ```bash
    terraform init
    ```

5.  **Review the deployment plan**:

    ```bash
    terraform plan
    ```

6.  **Apply the configuration**:
    ```bash
    terraform apply
    ```
    Enter `yes` to approve the deployment. The necessary resources will be created in your AWS account.

## How to Use (API Guide)

The deployed Lambda function is invoked via an `event` object. The `operation` key distinguishes between 'analyze' and 'execute' tasks.

### 1. Analyze Volumes

Analyze EBS volumes to receive a report with optimization recommendations.

- **Operation**: `analyze`
- **Parameters**:
  - `region` (required): The AWS region to analyze (e.g., `us-east-1`).
  - `volume_ids` (optional): A list of specific volume IDs to analyze. If omitted, all volumes in the region will be analyzed.

#### Example Request (Analyze all volumes)

```json
{
  "operation": "analyze",
  "region": "us-east-1"
}
```

#### Example Request (Analyze specific volumes)

```json
{
  "operation": "analyze",
  "region": "us-east-1",
  "volume_ids": ["vol-0123456789abcdef0", "vol-fedcba9876543210f"]
}
```

#### Example Response

The response consists of a `summary` and a `results` array containing individual volume analysis.

```json
{
  "summary": {
    "total_volumes_processed": 5,
    "idle_detected_count": 1,
    "overprovisioned_detected_count": 2,
    "disk_usage_unavailable_count": 0,
    "total_estimated_monthly_savings": 15.75
  },
  "results": [
    {
      "volume_id": "vol-01234idle12345678",
      "is_idle": true,
      "idle_reason": "Volume is in 'available' state and not attached to any instance.",
      "recommendation": "Volume is idle. Consider deleting or creating a snapshot before deletion.",
      "estimated_monthly_savings": 5.5,
      "...": "..."
    },
    {
      "volume_id": "vol-09876overprov54321",
      "is_idle": false,
      "is_overprovisioned": true,
      "overprovisioned_reason": "Low disk utilization detected...",
      "current_size_gb": 100,
      "recommended_size_gb": 30,
      "recommendation": "Resize volume to 30GB to save an estimated $10.25 per month.",
      "estimated_monthly_savings": 10.25,
      "...": "..."
    },
    {
      "volume_id": "vol-goodvolume123456",
      "is_idle": false,
      "is_overprovisioned": false,
      "recommendation": "Volume does not appear to be idle or over-provisioned.",
      "estimated_monthly_savings": 0,
      "...": "..."
    }
  ]
}
```

### 2. Execute Recommended Actions

Execute a recommendation from the analysis report.

- **Operation**: `execute`
- **Parameters**:
  - `region` (required): The AWS region where the action will be performed.
  - `volume_id` (required): The ID of the volume to modify.
  - `action_type` (required): The type of action to perform.
  - `volume_info` (required): The detailed information object for the volume, obtained from the 'analyze' step. This provides necessary context (e.g., recommended size/type) for the action.

#### Available `action_type` values

- `snapshot_only`: Creates a snapshot of the volume.
- `snapshot_and_delete`: For idle volumes, creates a snapshot and then deletes the volume.
- `change_type`: Changes the volume type (e.g., from `gp2` to `gp3`).
- `resize`: Modifies the volume size. (Note: This is only for increasing size; a safety mechanism prevents shrinking).
- `change_type_and_resize`: Modifies both the volume type and size in a single operation.

#### Example Request (Delete an idle volume)

```json
{
  "operation": "execute",
  "region": "us-east-1",
  "volume_id": "vol-01234idle12345678",
  "action_type": "snapshot_and_delete",
  "volume_info": {
    "volume_id": "vol-01234idle12345678",
    "is_idle": true,
    "...": "..."
  }
}
```

#### Example Request (Modify an over-provisioned volume)

_Note: The `resize` action is not supported by AWS for decreasing volume size. This tool includes a safety feature to prevent attempts to shrink volumes. This example shows a `change_type` action._

```json
{
  "operation": "execute",
  "region": "us-east-1",
  "volume_id": "vol-09876overprov54321",
  "action_type": "change_type",
  "volume_info": {
    "volume_id": "vol-09876overprov54321",
    "is_overprovisioned": true,
    "volume_type": "gp2",
    "recommended_type": "gp3",
    "...": "..."
  }
}
```

#### Example Response

```json
{
  "volume_id": "vol-01234idle12345678",
  "action_type": "snapshot_and_delete",
  "success": true,
  "timestamp": "2023-10-27T10:00:00.123456",
  "details": {
    "snapshot_id": "snap-0abcdef1234567890",
    "action": "Snapshot creation and volume deletion request completed.",
    "note": "The operations will continue in the background."
  },
  "status": "delete_initiated"
}
```

## Configuration

Analysis criteria are centrally managed in `code/config.py`. You can adjust the following values as needed:

- `EBS_METRIC_PERIOD`: The period for CloudWatch metric collection, in seconds.
- `EBS_IDLE_VOLUME_CRITERIA`: Thresholds for identifying idle volumes (e.g., days to check, idle time percentage).
- `EBS_OVERPROVISIONED_CRITERIA`: Thresholds for identifying over-provisioned volumes (e.g., disk usage percentage, resize buffer).
- `EBS_PRICING`: Region-specific EBS pricing information. **For production use, it is highly recommended to fetch dynamic, up-to-date pricing using the AWS Price List API.**

## Core Logic and Safety Features

- **Accurate Data via SSM**: For instances without the CloudWatch Agent, the tool uses SSM Run Command to directly query OS-level disk usage, increasing the accuracy of the analysis.
- **Root Volume Protection**: The `_is_root_volume` helper function in `executor.py` checks instance metadata to determine if a volume is a root device. If a dangerous action like `snapshot_and_delete` is requested for a root volume, the execution is automatically skipped and a warning is logged.
- **Asynchronous Operation Handling**: Actions like modifying a volume or creating a snapshot are handled asynchronously by AWS. This tool initiates the request and returns a response immediately, preventing Lambda function timeouts.

## Contributing

Contributions are welcome! Whether it's bug reports, feature suggestions, or code contributions, all forms of help are appreciated.

1.  Fork this repository.
2.  Create a new feature branch (`git checkout -b feature/AmazingFeature`).
3.  Commit your changes (`git commit -m 'Add some AmazingFeature'`).
4.  Push to the branch (`git push origin feature/AmazingFeature`).
5.  Open a Pull Request.

## License

This project is distributed under the MIT License. See the `LICENSE` file for more information.
