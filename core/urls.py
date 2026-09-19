from django.urls import path
from .views import (ReportCaseView, home_view, dashboard_home,
                    dashboard_data_api, analytics_view, analytics_data_api,
                    settings_view, ml_explained_view)

urlpatterns = [
    path('api/report-case/', ReportCaseView.as_view(), name='report-case'),
    path('api/dashboard-data/', dashboard_data_api, name='dashboard_data_api'),
    path('api/analytics-data/', analytics_data_api, name='analytics_data_api'),
    path('', home_view, name='home'),
    path('dashboard/', dashboard_home, name='dashboard_home'),
    path('analytics/', analytics_view, name='analytics_view'),
    path('settings/', settings_view, name='settings_view'),
    path('ml-explained/', ml_explained_view, name='ml_explained_view'),
]