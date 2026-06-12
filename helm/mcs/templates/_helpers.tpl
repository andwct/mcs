{{- define "mcs.fullname" -}}
{{- .Values.fullnameOverride | default .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "mcs.labels" -}}
app.kubernetes.io/name: mcs
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "mcs.selectorLabels" -}}
app.kubernetes.io/name: mcs
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "mcs.envConfigMapName" -}}
{{ include "mcs.fullname" . }}-env-config
{{- end }}

{{- define "mcs.productConfigMapName" -}}
{{ include "mcs.fullname" . }}-product-config
{{- end }}
