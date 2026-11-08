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
