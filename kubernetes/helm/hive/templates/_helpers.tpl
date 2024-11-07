{{/*
Expand the name of the chart.
*/}}
{{- define "hiveMetastore.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "hiveMetastore.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "hiveMetastore.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "hiveMetastore.labels" -}}
helm.sh/chart: {{ include "hiveMetastore.chart" . }}
{{ include "hiveMetastore.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "hiveMetastore.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hiveMetastore.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "hiveMetastore.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "hiveMetastore.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
This helper will change when users deploy a different image.
*/}}
{{ define "hiveMetastoreImage" -}}
{{ printf "%s:%s" (.Values.image.repository) (.Values.image.tag | default .Chart.AppVersion ) }}
{{- end }}

{{/*
Create the name of the initialize database job service account to use
*/}}
{{- define "initDatabaseJob.serviceAccountName" -}}
{{- if .Values.initDatabaseJob.serviceAccount.create -}}
  {{ default (printf "%s-init-database-job" (include "hiveMetastore.fullname" .)) .Values.initDatabaseJob.serviceAccount.name }}
{{- else -}}
  {{ default "default" .Values.initDatabaseJob.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{- define "imagePullSecret" -}}
{{ default (printf "%s-registry" .Release.Name) .Values.registry.secretName }}
{{- end }}
