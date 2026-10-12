# OfficePulse

OfficePulse is an IoT environmental analytics platform that covers the complete lifecycle of real-world data: physical capture, local decision-making on an ESP32, secure transport to AWS, historical storage, data processing, anomaly detection, and visualization.

The repository represents a completed MVP and contains a sample dataset obtained from real hardware. The documented numerical results correspond to the snapshot currently included; when new sessions are downloaded and the pipeline is rebuilt, those values will change.

## Use case

An office, classroom, laboratory, or workspace needs to record temperature, humidity, and lighting in order to preserve a historical record, verify measurement quality, and analyze environmental changes. OfficePulse instruments this scenario with a physical node and maintains traceability from each reading to the dashboard.

The LED demonstrates an edge control decision; it does not control HVAC or ventilation.

## What the project demonstrates

Sensors -> ESP32 and edge logic -> JSON -> MQTT/TLS
-> AWS IoT Core -> IoT Rule -> Amazon S3 RAW
-> boto3 -> Polars validation -> CURATED / QUARANTINE
-> Parquet -> DuckDB / SQL -> feature engineering
-> anomaly detection -> Streamlit

The flow integrates firmware, IoT security, AWS services, data engineering, quality controls, analytics, and a local application without introducing infrastructure that the MVP does not need.

## Architecture

![OfficePulse Architecture](Architecture.png)

The solution is divided into three layers:

- **Device:** DHT11 and LDR connected to an ESP32; acquisition, traceability, and local LED control.
- **AWS Cloud:** X.509 authentication, MQTT/TLS reception through AWS IoT Core, enrichment through an IoT Rule, RAW storage in S3, and observability with CloudWatch.
- **Data & Analytics:** incremental download, validation, Parquet, DuckDB queries, temporal features, anomaly detection, and Streamlit dashboard.

## Hardware

- ESP32 DevKit V1 with an ESP32-WROOM-32 module.
- DHT11 sensor for temperature and humidity.
- LDR sensor for lighting.
- LED as an actuator.
- Two 10 kΩ resistors.
- One 330 Ω resistor.
- Breadboard and connections.

The prototype uses `DHT_PIN = 15`, `LDR_PIN = 34`, `LED_PIN = 23`, and `DARK_THRESHOLD = 200`. These values can be adjusted in `src/main.cpp` for a different setup while respecting the electrical capabilities of the hardware being used.

## Edge processing

The ESP32 applies this rule for the LDR sensor without depending on AWS:

```text
light_raw < 200  -> light_state = "dark"   -> LED ON
light_raw >= 200 -> light_state = "bright" -> LED OFF
```

By default, firmware version `0.3.0` publishes one reading per minute using device/client ID `office-node-01` on `officepulse/office-node-01/telemetry`. A custom deployment can change `DEVICE_ID`, `AWS_IOT_CLIENT_ID`, `AWS_IOT_TOPIC`, and `READING_INTERVAL_MS` in `src/main.cpp`; the certificate, IoT Policy, IoT Rule, and S3 prefix must allow the same identifiers.

## Electronic circuit

![OfficePulse Electronic Schematic](Circuit.svg)

The schematic represents the connections used by OfficePulse. The ESP32 DevKit V1, based on the ESP32-WROOM-32 module, uses 3.3 V logic. The DHT11 DATA pin is connected to GPIO15 and uses a 10 kΩ pull-up resistor. The LDR is connected to GPIO34 through a voltage divider with a 10 kΩ resistor. The LED is controlled from GPIO23 through the 330 Ω current-limiting resistor shown in the schematic.

## Telemetry

Each message identifies the device, the experimental session, and the physical boot. The following values are illustrative and should be replaced with the identifiers and measurements from each deployment:

```json
{
  "device_id": "office-node-01",
  "session_id": "baseline_01",
  "boot_id": 1234567890,
  "sequence_number": 12,
  "device_timestamp_ms": 1787302000000,
  "uptime_ms": 720000,
  "dht_valid": true,
  "temperature_c": 30.4,
  "humidity_pct": 39.0,
  "light_raw": 356,
  "light_state": "bright",
  "led_active": false,
  "wifi_rssi": -61,
  "firmware_version": "0.3.0"
}
```

`boot_id`, `sequence_number`, `device_timestamp_ms`, and `uptime_ms` make it possible to detect restarts, gaps, and ordering issues. Ingestion adds `aws_ingest_ts` and `mqtt_topic`, which are used to measure latency and preserve the origin of each message.

## AWS and security

OfficePulse uses only:

- **AWS IoT Core** for device connectivity and MQTT message reception.
- **AWS IoT Rules Engine** for routing and enriching telemetry.
- **Amazon S3** as the immutable RAW layer.
- **AWS IAM** for local access through AWS CLI and boto3.
- **Amazon CloudWatch** for basic observability of the IoT flow.
- **AWS Budgets** for cost control.

The device authenticates using MQTT over TLS, an X.509 certificate, and an AWS IoT Policy. The pipeline uses the boto3 credential chain and does not contain access keys in the code. Local files containing Wi-Fi credentials, endpoint information, certificates, and the private key are excluded from Git and have publishable templates in `include/*.example.h`.

AWS resources are not created automatically from this repository. To use your own AWS account, the following resources must be prepared once:

1. An AWS IoT Thing, an active certificate, and an IoT Policy with permissions limited to the selected client ID and topic.
2. An S3 bucket to preserve RAW messages.
3. An IoT Rule that receives telemetry, adds the fields required by the pipeline, and writes each message as an independent object in S3.
4. A role that AWS IoT can assume with `s3:PutObject` permission on the destination prefix.
5. A local identity with `s3:ListBucket` and `s3:GetObject` permissions to run `download_raw.py`.

For the default topic, an equivalent IoT Rule configuration is:

```sql
SELECT
    *,
    timestamp() AS aws_ingest_ts,
    topic() AS mqtt_topic
FROM 'officepulse/+/telemetry'
```

The S3 action can use a key such as `raw/${topic(2)}/${timestamp()}.json`. If the topic or key structure is changed, `PREFIX` in `data-pipeline/src/download_raw.py` must also be updated. Bucket names, account names, users, profiles, Things, certificates, and roles belong to each user's environment and should not be copied literally from another deployment.

References: [create an IoT Rule](https://docs.aws.amazon.com/iot/latest/developerguide/iot-create-rule.html), [AWS IoT SQL functions](https://docs.aws.amazon.com/iot/latest/developerguide/iot-sql-functions.html), and [S3 action for an IoT Rule](https://docs.aws.amazon.com/iot/latest/developerguide/s3-rule-action.html).

## Data Engineering

The pipeline preserves the RAW layer and builds analytical datasets in stages:

```text
JSON RAW -> schema validation -> transformation
-> primary boot selection per session
-> CURATED + QUARANTINE -> Parquet -> DuckDB / SQL
```

For each `session_id`, the `boot_id` with the highest number of records is considered the primary run. Records from secondary boots are technically valid, but they are stored in QUARANTINE with the reason `non_primary_boot_for_session`; they are not deleted or presented as corrupted data. Therefore, the record counts depend on the captures available in `data/raw`.

## Data quality of the included snapshot

These values describe the files currently included and are not constants of the pipeline. They are recalculated when `build_dataset.py` is executed.

| Check | Result |
|---|---:|
| RAW JSON | 354 |
| Valid / corrupted JSON | 354 / 0 |
| CURATED / QUARANTINE | 316 / 38 |
| Duplicates | 0 |
| Sequence gaps | 0 |
| Estimated missing messages | 0 |
| Invalid DHT readings | 0 |
| Invalid humidity | 0 |
| Invalid lighting | 0 |
| `light_state` errors | 0 |
| LED logic errors | 0 |

Keeping QUARANTINE separate protects the analytical series without sacrificing traceability, auditability, or RAW data integrity.

## Sessions in the included snapshot

| Session | Purpose | CURATED records |
|---|---|---:|
| `baseline_01` | Normal behavior for training | 47 |
| `baseline_02` | Independent normal behavior for validation | 50 |
| `light_transition_01` | Controlled lighting changes | 93 |
| `temperature_transition_01` | Controlled thermal disturbance | 126 |

The baseline sessions were captured approximately once per minute, while the controlled tests were captured approximately every 10 seconds. To prevent sampling frequency from biasing the analysis, all sessions were aggregated into one-minute windows. The current snapshot, excluding the pending demonstration capture, produces 135 resampled observations and 131 ML-ready observations.

## Step-by-step demonstration: `baseline_03`

`baseline_03` is reserved for the complete project demonstration in the video and for the final documentation. This session will show the entire flow from beginning to end: configuring `SESSION_ID` in the firmware, capturing data with the ESP32, sending it through AWS IoT Core, storing it in S3, downloading the RAW data, building the dataset, generating features, detecting anomalies, and visualizing the results in the dashboard.

The record counts and metrics for `baseline_03` are not fixed in this README before the final capture is performed. They will be obtained by following the **How to run the project** section and documented after the demonstration has been completed and validated. This ensures that the published results correspond exactly to the execution shown in the video.

## Feature engineering

The analysis models temporal dynamics using deltas, per-minute rates, and absolute change magnitudes for temperature, humidity, and lighting. The detectors mainly use:

- `temperature_change_magnitude`
- `humidity_change_magnitude`
- `light_change_magnitude`

`light_state` and `led_active` are not used as inputs because they already encode the edge decision and would create conceptual leakage. `elapsed_minutes`, `samples_in_minute`, `led_active_ratio`, and `dark_ratio` are preserved as analytical context.

## Anomaly detection

The functional prototype compares three outputs:

1. A robust statistical detector based on median, MAD, fallback to standard deviation when `MAD = 0`, and threshold `3.5`.
2. Isolation Forest trained only on the 46 ML-ready observations from `baseline_01` (`n_estimators = 300`, `contamination = "auto"`, `random_state = 42`). The code uses the 95th percentile of the training scores as the threshold.
3. Consensus, activated only when both detectors flag the same observation.

Results obtained with the included snapshot:

| Evaluation group | ML-ready observations | Consensus flag rate |
|---|---:|---:|
| Training normal (`baseline_01`) | 46 | 2.17 % |
| Validation normal (`baseline_02`) | 49 | 2.04 % |
| Lighting test | 15 | 53.33 % |
| Temperature test | 21 | 4.76 % |

These values are **flag rates**, not accuracy, precision, or recall: there is no minute-by-minute ground truth. They change when new `baseline_N` sessions are added to the validation group. The lighting transition is clearly detected; the thermal disturbance produces fewer flags because its change is gradual. The model is not presented as production-ready or as evidence of generalization.

## Dashboard

The local dashboard consumes processed Parquet files using Streamlit, Plotly, and Polars; it does not use pandas.

- **Overview:** temperature, humidity, light, Wi-Fi RSSI, AWS latency, LED state, and trends.
- **Historical Analysis:** session comparison, historical data, and data quality.
- **Anomaly Detection:** statistical and Isolation Forest detectors, consensus, scores, flagged records, and session filters.

It is not a production real-time dashboard or a publicly deployed application.

## Observability

CloudWatch provides basic visibility into connectivity, publishing, and rule execution through `PublishIn.Success`, `Connect.Success`, `RulesExecuted`, and the success or failure metrics of the S3 action.

## Illustrative cost estimate

As a reference for the MVP, AWS Pricing Calculator was used to model one ESP32 connected 24/7, publishing one message per minute for 30 days in the `us-east-1` region:

- 43,200 messages, RulesTriggered, and ActionsExecuted per month.
- Approximately 43,200 S3 PUT operations and 43,200 S3 GET operations in the calculated scenario.
- Approximately **USD 3.29/month** at list price: IoT Core ~USD 0.06, S3 ~USD 0.23, and CloudWatch dashboard ~USD 3.00.

This figure is not a quote or a guaranteed cost for other users. The actual amount depends on the selected region, message volume, storage, queries, metric retention, taxes, applicable Free Tier, and current pricing. Each deployment should generate its own estimate before creating resources.

## Technology stack

| Area | Technologies |
|---|---|
| Firmware / Edge | C++, Arduino, PlatformIO, DHT, ArduinoJson, WiFiClientSecure, PubSubClient, NTP |
| Cloud | AWS IoT Core, IoT Rules Engine, S3, IAM, CloudWatch, Budgets |
| Data | Python, boto3, Polars, PyArrow, Parquet |
| Analytics / ML | DuckDB, SQL, NumPy, scikit-learn, Matplotlib |
| Visualization | Streamlit, Plotly |

## Repository structure

```text
officepulse/
|-- src/                         # ESP32 firmware
|-- include/                     # publishable IoT configuration templates
|-- data-pipeline/
|   |-- src/                     # ingestion, validation, EDA, features, and ML
|   |-- sql/                     # DuckDB analysis
|   |-- dashboard/               # Streamlit application
|   `-- data/                    # RAW, CURATED, QUARANTINE, and results
|-- Architecture.png
|-- Circuit.svg
|-- platformio.ini
`-- README.md
```

## How to run the project

There are two ways to use the repository:

- **Reproduce the included analysis:** only Python and the files in `data-pipeline/data/raw` are required; no hardware, AWS account, or credentials are needed.
- **Capture your own data:** requires the described hardware, PlatformIO, a Wi-Fi network, your own AWS IoT/S3 resources, and AWS CLI v2 to download the messages.

The following commands are written for PowerShell on Windows. On Linux or macOS, the virtual environment executable paths must be adapted.

### 1. Prepare and run the firmware

VSCode with the PlatformIO extension and a device configured in AWS IoT are required.

From the repository root:

1. Copy `include/secrets.example.h` as `include/secrets.h` and fill in the Wi-Fi credentials and endpoint for your own AWS IoT environment.
2. Copy `include/aws_certs.example.h` as `include/aws_certs.h` and fill in the CA, certificate, and private key issued for your own device.
3. During the initial setup, verify in `src/main.cpp` that `DEVICE_ID`, `AWS_IOT_CLIENT_ID`, and `AWS_IOT_TOPIC` match the Thing, certificate, policy, and rule created in AWS.
4. Set `SESSION_ID` in `src/main.cpp` before each capture. For a normal session, use the `baseline_N` format, for example:

   ```cpp
   constexpr char SESSION_ID[] = "baseline_04";
   ```

5. Build, upload the firmware, and open the serial monitor:

   ```powershell
   pio run
   pio run --target upload
   pio device monitor
   ```

6. Keep the device powered on during the capture period. Telemetry is published to AWS IoT Core and stored as RAW JSON in Amazon S3.

`baseline_N` sessions are automatically incorporated into the pipeline. `baseline_01`
remains the training dataset, while any other `baseline_N` is classified
as normal validation data. It is not necessary to modify the Python files when capturing
`baseline_03`, `baseline_04`, or later sessions.

### 2. Prepare the local environment

From `data-pipeline/`, create the virtual environment and install the dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

`requirements.txt` also installs AWS CRT through `boto3[crt]`, which is required for Boto3 to use the temporary credentials generated by `aws login`. Creating the environment and installing dependencies is only necessary the first time or when `requirements.txt` changes.

### 3. Configure AWS access

This step is only required to download new captures from an S3 bucket. It can be skipped entirely when reproducing the RAW data already included in the repository.

Before continuing:

1. Install [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) separately. It is a system application and is not part of `requirements.txt`. Verify the installation with:

   ```powershell
   aws --version
   ```

   The output should begin with `aws-cli/2`.

2. Have access to the bucket containing the telemetry.
3. Configure `BUCKET_NAME`, `PREFIX`, and the region in `src/download_raw.py` so they match your own AWS environment.
4. Use an identity with permissions to list the bucket and download its objects, at minimum `s3:ListBucket` and `s3:GetObject`.

The bucket, profile, and credentials only need to be configured once per environment. After that, each new `baseline_N` capture only requires changing `SESSION_ID`, uploading the firmware, and repeating the download and processing steps.

The profile name is local and can be chosen freely. The following examples use `officepulse-local`:

```powershell
$profileName = "officepulse-local"
```

#### Option A: browser-based sign-in

This option uses the console credentials of an IAM user and generates temporary credentials. It does not require creating a permanent access key.

```powershell
aws login --profile $profileName
$env:AWS_PROFILE = $profileName
aws sts get-caller-identity --profile $profileName
```

The browser will request the Account ID or account alias, IAM user name, password, and MFA if enabled. CRT support that allows Boto3 to use this session is already installed through `requirements.txt`.

Reference: [local sign-in with `aws login`](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sign-in.html).

If the session expires, run:

```powershell
aws login --profile $profileName
```

#### Option B: IAM user access key

This option configures a profile using an Access Key ID and Secret Access Key:

```powershell
aws configure --profile $profileName
$env:AWS_PROFILE = $profileName
aws sts get-caller-identity --profile $profileName
```

`aws configure` will request the Access Key ID, Secret Access Key, default region, and output format. Credentials must remain outside the repository and must never be written into the code or added to Git.

Reference: [AWS CLI authentication with IAM user credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-authentication-user.html).

In both options, `get-caller-identity` allows you to verify which identity will be used before accessing S3. The `AWS_PROFILE` variable must be set again when opening a new PowerShell terminal.

### 4. Download the new capture

After authenticating the selected profile, from `data-pipeline/`:

```powershell
.\.venv\Scripts\python.exe src\download_raw.py
```

`download_raw.py` downloads new JSON files from S3 into `data/raw` and skips files that already exist locally.

### 5. Build and analyze the dataset

Run the scripts in the following order from `data-pipeline/`:

```powershell
.\.venv\Scripts\python.exe src\build_dataset.py
.\.venv\Scripts\python.exe src\eda.py
.\.venv\Scripts\python.exe sql\analyze_baseline.py
.\.venv\Scripts\python.exe src\feature_engineering.py
.\.venv\Scripts\python.exe src\anomaly_detection.py
```

Each stage performs the following:

1. `build_dataset.py` validates the JSON files, automatically recognizes all `baseline_N` sessions, selects the primary run, and generates `officepulse_all.parquet` plus one Parquet file per session.
2. `eda.py` generates statistics and charts for all detected sessions.
3. `analyze_baseline.py` automatically selects the `baseline_N` with the highest number and runs descriptive analysis in DuckDB.
4. `feature_engineering.py` resamples the sessions, creates temporal variables, and assigns training or validation groups.
5. `anomaly_detection.py` trains on `baseline_01`, evaluates the validation sessions and controlled tests, and stores the scores and predictions.

### 6. Open the dashboard

After generating the datasets and model results:

```powershell
.\.venv\Scripts\python.exe -m streamlit run dashboard\app.py
```

The dashboard automatically discovers the available sessions and displays names such as “Reference 03” or “Baseline 03”, depending on the selected language.

## Limitations

- The normal training dataset contains 46 ML-ready observations and covers only a small number of controlled sessions.
- There is no minute-by-minute ground truth for calculating supervised metrics.
- The DHT11 is appropriate for the MVP, not an industrial-grade sensor.
- The dashboard runs locally on processed Parquet data and does not represent real-time streaming.
- AWS resource configuration is not versioned as infrastructure as code; the repository contains firmware, pipeline, data, and the architecture diagram.
- Anomaly detection is a functional prototype for this dataset, not a production model.
