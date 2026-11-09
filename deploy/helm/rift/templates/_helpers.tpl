{{- define "rift.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{- define "rift.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rift.labels" -}}
app.kubernetes.io/name: {{ include "rift.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{- define "rift.selector" -}}
app.kubernetes.io/name: {{ include "rift.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "rift.secret" -}}
{{- .Values.credentials.existingSecret | default (include "rift.fullname" .) -}}
{{- end -}}

{{- define "rift.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- .Values.serviceAccount.name | default (include "rift.fullname" .) -}}
{{- else -}}
{{- .Values.serviceAccount.name | default "default" -}}
{{- end -}}
{{- end -}}

{{- define "rift.serveSelector" -}}
app.kubernetes.io/name: {{ include "rift.name" . }}-serve
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* What every gateway is started with: where the bucket is, and how it serves. */}}
{{- define "rift.servingArgs" -}}
- gateway
- --listen=0.0.0.0:9100
- --backend={{ .Values.backend.endpoint }}
- --bucket={{ .Values.backend.bucket }}
- --region={{ .Values.backend.region }}
- --interval={{ .Values.compaction.interval }}
- --max-body-mb={{ .Values.limits.maxBodyMB }}
- --max-requests={{ .Values.limits.maxRequests }}
- --max-cold-reads={{ .Values.limits.maxColdReads }}
- --max-spill-gb={{ .Values.limits.maxSpillGB }}
- --client-timeout-secs={{ .Values.limits.clientTimeoutSecs }}
- --max-connections={{ .Values.limits.maxConnections }}
{{- if .Values.accessLog }}
- --access-log
{{- end }}
{{- if .Values.tls.secretName }}
- --tls-cert=/etc/rift/tls/tls.crt
- --tls-key=/etc/rift/tls/tls.key
{{- end }}
{{- with .Values.backend.redirectEndpoint }}
- --redirect-endpoint={{ . }}
{{- end }}
{{- if not .Values.compaction.hotCopy }}
- --no-hot-copy
{{- end }}
{{- with .Values.threads }}
- --threads={{ . }}
{{- end }}
{{- end -}}
