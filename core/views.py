import math
from collections import defaultdict

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from django.shortcuts import render
from django.http import JsonResponse
from django.db.models import Count, Avg, Q
from django.db.models.functions import TruncMinute

from .kafka_producer import send_case_to_kafka
from .models import MalariaCase, CasePrediction, OutbreakPrediction, FeatureImportance


TRAINING_METRICS = {
    "case":     {"AUC": None, "Accuracy": None, "F1": None, "Precision": None, "Recall": None},
    "outbreak": {"AUC": None, "F1": None, "Precision": None, "Recall": None},
}

REGION_COLORS = ['#0891b2', '#7c3aed', '#d97706', '#059669', '#db2777', '#2563eb', '#65a30d']


# ============================================================
# API — Ingestion (Kafka Producer)
# ============================================================

class ReportCaseView(APIView):
    def post(self, request):
        data = request.data
        required_fields = [
            "case_id", "timestamp", "region_id", "district",
            "gps_lat", "gps_lon", "patient_age", "gender",
            "symptoms", "rapid_test_result", "body_temperature",
            "ambient_temperature", "humidity", "rainfall_mm"
        ]
        missing = [f for f in required_fields if f not in data]
        if missing:
            return Response({"error": f"Missing fields: {missing}"}, status=status.HTTP_400_BAD_REQUEST)

        send_case_to_kafka(data)
        return Response({"status": "sent to kafka"}, status=status.HTTP_201_CREATED)


# ============================================================
# Helper — construit toutes les données de la vue Opérationnelle
# (utilisé à la fois par la page HTML et par l'endpoint JSON temps réel)
# ============================================================

def _build_operational_context():
    recent_cases = MalariaCase.objects.order_by('-timestamp')[:20]

    region_summary = list(
        MalariaCase.objects.values('region_id')
        .annotate(
            total_cases=Count('case_id'),
            positive_cases=Count('case_id', filter=Q(rapid_test_result='positive')),
        )
        .order_by('-total_cases')
    )

    total_cases = MalariaCase.objects.count()
    total_positive = MalariaCase.objects.filter(rapid_test_result='positive').count()

    positivity = [
        round(r['positive_cases'] / r['total_cases'] * 100, 1) if r['total_cases'] else 0
        for r in region_summary
    ]

    timeline = list(
        MalariaCase.objects.annotate(bucket=TruncMinute('timestamp'))
        .values('bucket')
        .annotate(total=Count('case_id'),
                  positive=Count('case_id', filter=Q(rapid_test_result='positive')))
        .order_by('-bucket')[:30]
    )
    timeline.reverse()

    age_bins = [(0, 4), (5, 14), (15, 24), (25, 34), (35, 44), (45, 59), (60, 150)]
    agg = {}
    for i, (lo, hi) in enumerate(age_bins):
        cond = Q(patient_age__gte=lo, patient_age__lte=hi)
        agg[f'b{i}_t'] = Count('case_id', filter=cond)
        agg[f'b{i}_p'] = Count('case_id', filter=cond & Q(rapid_test_result='positive'))
    age_res = MalariaCase.objects.aggregate(**agg)
    age_labels = [f"{lo}-{hi}" if hi < 150 else f"{lo}+" for lo, hi in age_bins]
    age_total = [age_res[f'b{i}_t'] for i in range(len(age_bins))]
    age_pos = [age_res[f'b{i}_p'] for i in range(len(age_bins))]

    rain_bins = [(0, 2), (2, 5), (5, 10), (10, 20), (20, 10000)]
    agg = {}
    for i, (lo, hi) in enumerate(rain_bins):
        cond = Q(rainfall_mm__gte=lo, rainfall_mm__lt=hi)
        agg[f'r{i}_t'] = Count('case_id', filter=cond)
        agg[f'r{i}_p'] = Count('case_id', filter=cond & Q(rapid_test_result='positive'))
    rain_res = MalariaCase.objects.aggregate(**agg)
    rain_labels = [f"{lo}-{hi} mm" if hi < 10000 else f"{lo}+ mm" for lo, hi in rain_bins]
    rain_rate = [
        round(rain_res[f'r{i}_p'] / rain_res[f'r{i}_t'] * 100, 1) if rain_res[f'r{i}_t'] else 0
        for i in range(len(rain_bins))
    ]

    gender_map = {'M': 'Male', 'F': 'Female'}
    gender_rows = list(MalariaCase.objects.values('gender').annotate(n=Count('case_id')).order_by('-n'))

    charts = {
        'region_labels': [r['region_id'] for r in region_summary],
        'region_totals': [r['total_cases'] for r in region_summary],
        'region_positives': [r['positive_cases'] for r in region_summary],
        'pie': [total_positive, total_cases - total_positive],
        'positivity': positivity,
        'timeline_labels': [t['bucket'].strftime('%H:%M') for t in timeline],
        'timeline_total': [t['total'] for t in timeline],
        'timeline_positive': [t['positive'] for t in timeline],
        'age_labels': age_labels, 'age_total': age_total, 'age_pos': age_pos,
        'rain_labels': rain_labels, 'rain_rate': rain_rate,
        'gender_labels': [gender_map.get(g['gender'], str(g['gender'])) for g in gender_rows],
        'gender_values': [g['n'] for g in gender_rows],
    }

    recent_cases_data = [{
        'case_id': c.case_id,
        'region_id': c.region_id,
        'district': c.district,
        'patient_age': c.patient_age,
        'gender': c.get_gender_display(),
        'rapid_test_result': c.rapid_test_result,
        'timestamp': c.timestamp.strftime('%Y-%m-%d %H:%M:%S') if c.timestamp else '',
    } for c in recent_cases]

    return {
        'recent_cases': recent_cases,
        'recent_cases_data': recent_cases_data,
        'region_summary': region_summary,
        'total_cases': total_cases,
        'total_positive': total_positive,
        'charts': charts,
    }


# ============================================================
# Dashboard — Operational View (page HTML, premier chargement)
# ============================================================

def dashboard_home(request):
    ctx = _build_operational_context()
    return render(request, 'core/dashboard.html', ctx)


# ============================================================
# API JSON — interrogée en boucle (polling) par le navigateur
# pour mettre à jour le dashboard SANS recharger la page.
# ============================================================

def dashboard_data_api(request):
    ctx = _build_operational_context()
    return JsonResponse({
        'total_cases': ctx['total_cases'],
        'total_positive': ctx['total_positive'],
        'region_count': len(ctx['region_summary']),
        'region_summary': ctx['region_summary'],
        'recent_cases': ctx['recent_cases_data'],
        'charts': ctx['charts'],
    })


# ============================================================
# Analytics — résultats des 2 modèles MLlib
# ============================================================

def _build_analytics_context():
    preds = list(OutbreakPrediction.objects.order_by('date'))

    latest = {}
    for p in preds:
        latest[p.region_id] = p
    latest_by_region = sorted(latest.values(), key=lambda p: p.region_id)
    at_risk_regions = [p for p in latest_by_region if p.prediction == 1.0]

    n_outbreak = sum(1 for p in preds if p.prediction == 1.0)
    n_stable = len(preds) - n_outbreak

    stack = list(
        OutbreakPrediction.objects.values('region_id')
        .annotate(outbreak=Count('pk', filter=Q(prediction__gt=0.5)),
                  stable=Count('pk', filter=Q(prediction__lte=0.5)))
        .order_by('region_id')
    )

    dates = sorted({p.date for p in preds})[-30:]
    date_idx = {d: i for i, d in enumerate(dates)}
    regions = sorted({p.region_id for p in preds})
    grid = {r: [None] * len(dates) for r in regions}
    pcol = {r: [None] * len(dates) for r in regions}
    color_of = {r: REGION_COLORS[i % len(REGION_COLORS)] for i, r in enumerate(regions)}
    for p in preds:
        if p.date in date_idx:
            i = date_idx[p.date]
            grid[p.region_id][i] = p.positive_cases
            pcol[p.region_id][i] = '#dc2626' if p.prediction == 1.0 else color_of[p.region_id]
    timeline_datasets = [{
        'label': r, 'data': grid[r], 'borderColor': color_of[r],
        'backgroundColor': color_of[r], 'pointBackgroundColor': pcol[r],
        'pointRadius': 5, 'tension': 0.3, 'spanGaps': True,
    } for r in regions]
    try:
        timeline_labels = [d.strftime('%H:%M') for d in dates]
    except AttributeError:
        timeline_labels = [str(d) for d in dates]

    vals = sorted(p.positive_cases for p in preds)
    hist_labels, hist_counts, hist_colors, threshold = [], [], [], None
    if vals:
        threshold = vals[int(0.75 * (len(vals) - 1))]
        width = max(1, math.ceil((vals[-1] + 1) / 10))
        lo = 0
        while lo <= vals[-1]:
            hi = lo + width - 1
            hist_labels.append(str(lo) if width == 1 else f"{lo}-{hi}")
            hist_counts.append(sum(1 for v in vals if lo <= v <= hi))
            hist_colors.append('#dc2626' if lo > threshold else '#94a3b8')
            lo += width

    case_pred_summary = list(
        CasePrediction.objects.values('region_id')
        .annotate(avg_risk=Avg('prediction'), total=Count('case_id'))
        .order_by('-avg_risk')
    )

    cm, metrics_case = None, None
    fields = {f.name for f in CasePrediction._meta.get_fields()}
    if 'label' in fields:
        tp = CasePrediction.objects.filter(label__gt=0.5, prediction__gt=0.5).count()
        tn = CasePrediction.objects.filter(label__lte=0.5, prediction__lte=0.5).count()
        fp = CasePrediction.objects.filter(label__lte=0.5, prediction__gt=0.5).count()
        fn = CasePrediction.objects.filter(label__gt=0.5, prediction__lte=0.5).count()
        n = tp + tn + fp + fn
        prec = tp / (tp + fp) if (tp + fp) else 0
        rec = tp / (tp + fn) if (tp + fn) else 0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0
        cm = {'tp': tp, 'tn': tn, 'fp': fp, 'fn': fn}
        metrics_case = {
            'accuracy': round((tp + tn) / n * 100, 1) if n else 0,
            'precision': round(prec * 100, 1),
            'recall': round(rec * 100, 1),
            'f1': round(f1 * 100, 1),
            'n': n,
        }

    fi_case = list(FeatureImportance.objects.filter(model_name='case_risk').order_by('-importance'))
    fi_out = list(FeatureImportance.objects.filter(model_name='outbreak').order_by('-importance'))

    charts = {
        'latest_labels': [p.region_id for p in latest_by_region],
        'latest_totals': [p.total_cases for p in latest_by_region],
        'latest_positives': [p.positive_cases for p in latest_by_region],
        'status': [n_outbreak, n_stable],
        'stack_labels': [s['region_id'] for s in stack],
        'stack_outbreak': [s['outbreak'] for s in stack],
        'stack_stable': [s['stable'] for s in stack],
        'timeline_labels': timeline_labels,
        'timeline_datasets': timeline_datasets,
        'hist_labels': hist_labels, 'hist_counts': hist_counts,
        'hist_colors': hist_colors, 'threshold': threshold,
        'risk_labels': [c['region_id'] for c in case_pred_summary],
        'risk_values': [round(c['avg_risk'], 3) if c['avg_risk'] is not None else 0 for c in case_pred_summary],
        'fi_case_labels': [f.feature_name for f in fi_case],
        'fi_case_values': [round(f.importance * 100, 1) for f in fi_case],
        'fi_out_labels': [f.feature_name for f in fi_out],
        'fi_out_values': [round(f.importance * 100, 1) for f in fi_out],
    }

    return {
        'latest_by_region': latest_by_region,
        'at_risk_regions': at_risk_regions,
        'case_pred_summary': case_pred_summary,
        'cm': cm,
        'metrics_case': metrics_case,
        'training_case': [(k, v) for k, v in TRAINING_METRICS['case'].items() if v is not None],
        'training_outbreak': [(k, v) for k, v in TRAINING_METRICS['outbreak'].items() if v is not None],
        'threshold': threshold,
        'charts': charts,
        'at_risk_count': len(at_risk_regions),
    }


def analytics_view(request):
    return render(request, 'core/analytics.html', _build_analytics_context())


def analytics_data_api(request):
    ctx = _build_analytics_context()
    return JsonResponse({
        'charts': ctx['charts'],
        'at_risk_count': ctx['at_risk_count'],
        'at_risk_regions': [p.region_id for p in ctx['at_risk_regions']],
        'metrics_case': ctx['metrics_case'],
        'cm': ctx['cm'],
    })


# ============================================================
# Pages statiques
# ============================================================

def settings_view(request):
    return render(request, 'core/settings.html')


def ml_explained_view(request):
    return render(request, 'core/ml_explained.html')


def home_view(request):
    return render(request, 'core/home.html', {
        'total_cases': MalariaCase.objects.count(),
        'total_positive': MalariaCase.objects.filter(rapid_test_result='positive').count(),
        'regions': MalariaCase.objects.values('region_id').distinct().count(),
    })