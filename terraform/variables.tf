variable "env" {
  description = "Environment name: qa or prd"
  type        = string
  default = "qa"
}

variable "script_name_1" {
  default = "1-auna-dlake-qas-job-int-raw-mkfintermedio-gluejob-v2-executeloadraw-1.py"
}

variable "script_name" {
  /* default = "mkfintermedio-gluejob-v1-executeloadanalytics-3.py" */
  default = "3-auna-dlake-qas-job-int-analytics-mkfintermedio-gluejob-v2-executeloadanalytics-3.py"
}

variable "script_name_4" {
  default = "4-auna-dlake-qas-job-int-analytics-mkfintermedio-gluejob-v2-executeloadanalytics-4.py"
}

variable "glue_version" {
  description = "The version of glue to use"
  type        = string
  default     = "2.0"
}

variable "job_timeout" {
  description = "The job timeout in minutes. The default is 2880 minutes (48 hours)."
  type        = number
  default     = 60
}
variable "max_concurrent_runs" {
  description = "The maximum number of concurrent runs allowed for a job. The default is 1"
  type        = number
  default     = 1
}

variable "max_retries" {
  description = "The maximum number of times to retry this job if it fails"
  type        = number
  default     = 1
}

variable "number_of_workers" {
  description = "The number of workers of a defined workerType that are allocated when a job runs."
  type        = number
  default     = 5
}

variable "worker_type" {
  description = "The type of predefined worker that is allocated when a job runs. Accepts a value of Standard, G.1X, or G.2X"
  type        = string
  default     = "G.1X"
}