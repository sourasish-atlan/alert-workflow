package shared

import (
	"go.temporal.io/sdk/log"
	"go.uber.org/zap"
)

// ZapAdapter adapts zap logger to Temporal's logger interface
type ZapAdapter struct {
	logger *zap.Logger
}

func NewZapAdapter(logger *zap.Logger) *ZapAdapter {
	return &ZapAdapter{logger: logger}
}

func (z *ZapAdapter) Debug(msg string, keyvals ...interface{}) {
	z.logger.Sugar().Debugw(msg, keyvals...)
}

func (z *ZapAdapter) Info(msg string, keyvals ...interface{}) {
	z.logger.Sugar().Infow(msg, keyvals...)
}

func (z *ZapAdapter) Warn(msg string, keyvals ...interface{}) {
	z.logger.Sugar().Warnw(msg, keyvals...)
}

func (z *ZapAdapter) Error(msg string, keyvals ...interface{}) {
	z.logger.Sugar().Errorw(msg, keyvals...)
}

func (z *ZapAdapter) With(keyvals ...interface{}) log.Logger {
	return NewZapAdapter(z.logger.With(convertToZapFields(keyvals)...))
}

func convertToZapFields(keyvals []interface{}) []zap.Field {
	var fields []zap.Field
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			key := keyvals[i].(string)
			value := keyvals[i+1]
			fields = append(fields, zap.Any(key, value))
		}
	}
	return fields
}
