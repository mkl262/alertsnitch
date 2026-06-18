{{/*
Expand the name of the chart.
*/}}
{{- define "alertsnitch.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "alertsnitch.fullname" -}}
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
{{- define "alertsnitch.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "alertsnitch.labels" -}}
helm.sh/chart: {{ include "alertsnitch.chart" . }}
{{ include "alertsnitch.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "alertsnitch.selectorLabels" -}}
app.kubernetes.io/name: {{ include "alertsnitch.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Container spec — shared by the Deployment and StatefulSet workloads so the pod
definition lives in exactly one place.
*/}}
{{- define "alertsnitch.container" -}}
- name: {{ .Chart.Name }}
  image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
  imagePullPolicy: {{ .Values.image.pullPolicy }}
  ports:
    - name: http
      containerPort: 9567
      protocol: TCP
  livenessProbe:
    httpGet:
      path: /-/health
      port: http
    initialDelaySeconds: 5
    periodSeconds: 15
  readinessProbe:
    httpGet:
      path: /-/ready
      port: http
    initialDelaySeconds: 5
    periodSeconds: 10
  env:
    {{- range $key, $value := .Values.env }}
    - name: {{ $key }}
      value: {{ $value | quote }}
    {{- end }}
    {{- if .Values.secret.create }}
    {{- range $key, $value := .Values.secret.data }}
    - name: {{ $key }}
      valueFrom:
        secretKeyRef:
          name: {{ include "alertsnitch.fullname" $ }}
          key: {{ $key }}
    {{- end }}
    {{- end }}
  resources:
    {{- toYaml .Values.resources | nindent 4 }}
  {{- if .Values.persistence.enabled }}
  volumeMounts:
    - name: wal
      mountPath: {{ .Values.persistence.mountPath }}
  {{- end }}
{{- end }}

{{/*
Pod scheduling fields — shared by both workloads.
*/}}
{{- define "alertsnitch.podScheduling" -}}
{{- with .Values.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}
