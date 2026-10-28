package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/adonm/pgvs3"

type Shutdown func(context.Context) error

func Setup(ctx context.Context, version string, log *slog.Logger) (Shutdown, error) {

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") ||
		strings.EqualFold(os.Getenv("OTEL_TRACES_EXPORTER"), "none") ||
		(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" &&
			os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "") {
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP trace exporter: %w", err)
	}
	service := os.Getenv("OTEL_SERVICE_NAME")
	if service == "" {
		service = "pgvs3"
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		"",
		attribute.String("service.name", service),
		attribute.String("service.version", version),
	))
	if err != nil {
		return nil, fmt.Errorf("building tracing resource: %w", err)
	}
	ratio := 1.0
	if raw := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil || parsed < 0 || parsed > 1 {
			return nil, fmt.Errorf("OTEL_TRACES_SAMPLER_ARG must be between 0 and 1")
		}
		ratio = parsed
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tp)
	if log != nil {
		log.Info("tracing enabled", "service", service, "sample_ratio", ratio)
	}
	return tp.Shutdown, nil
}

func Middleware(next http.Handler) http.Handler {
	tracer := otel.Tracer(instrumentationName + "/http")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracer.Start(ctx, r.Method,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.request.method", r.Method),
				attribute.String("url.path", r.URL.Path),
				attribute.String("server.address", r.Host),
			),
		)
		defer span.End()
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(w.Header()))
		if sc := span.SpanContext(); sc.IsValid() {
			w.Header().Set("X-Trace-Id", sc.TraceID().String())
		}
		sw := &statusWriter{ResponseWriter: w}
		r = r.WithContext(ctx)
		next.ServeHTTP(sw, r)
		if sw.status == 0 {
			sw.status = http.StatusOK
		}
		if sw.status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(sw.status))
		}
		span.SetAttributes(attribute.Int("http.response.status_code", sw.status))
	})
}

func Inject(ctx context.Context, h http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(h))
}

type PGXTracer struct{}

func (PGXTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	ctx, _ = otel.Tracer(instrumentationName+"/postgres").Start(ctx, "postgresql.query",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.query.text", data.SQL),
		),
	)
	return ctx
}

func (PGXTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	if data.Err != nil {
		span.RecordError(data.Err)
		span.SetStatus(codes.Error, data.Err.Error())
	}
	span.SetAttributes(attribute.String("db.response.status_code", responseCode(data)))
	span.End()
}

func responseCode(data pgx.TraceQueryEndData) string {
	if data.Err == nil {
		return "OK"
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](data.Err); ok {
		return pgErr.Code
	}
	return "ERROR"
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
