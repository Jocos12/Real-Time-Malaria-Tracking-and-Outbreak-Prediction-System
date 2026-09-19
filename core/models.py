from django.db import models


class MalariaCase(models.Model):
    RESULT_CHOICES = [
        ('positive', 'Positive'),
        ('negative', 'Negative'),
    ]
    GENDER_CHOICES = [
        ('M', 'Male'),
        ('F', 'Female'),
    ]

    case_id = models.CharField(max_length=64, unique=True)
    timestamp = models.DateTimeField()
    region_id = models.CharField(max_length=50)
    district = models.CharField(max_length=100)
    gps_lat = models.FloatField()
    gps_lon = models.FloatField()

    patient_age = models.IntegerField()
    gender = models.CharField(max_length=1, choices=GENDER_CHOICES)
    symptoms = models.JSONField(default=list)
    rapid_test_result = models.CharField(max_length=10, choices=RESULT_CHOICES)

    body_temperature = models.FloatField()
    ambient_temperature = models.FloatField()
    humidity = models.FloatField()
    rainfall_mm = models.FloatField()

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "current_cases"
        managed = False
        indexes = [
            models.Index(fields=["region_id"]),
            models.Index(fields=["timestamp"]),
        ]

    def __str__(self):
        return f"{self.case_id} - {self.region_id} - {self.rapid_test_result}"


class CasePrediction(models.Model):
    case_id = models.CharField(max_length=100, primary_key=True)
    region_id = models.CharField(max_length=50)
    patient_age = models.IntegerField(null=True, blank=True)
    gender = models.CharField(max_length=10, null=True, blank=True)
    label = models.FloatField(null=True, blank=True)
    prediction = models.FloatField(null=True, blank=True)
    synced_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = "case_predictions"

    def __str__(self):
        return f"{self.case_id} -> {self.prediction}"


class OutbreakPrediction(models.Model):
    region_id = models.CharField(max_length=50)
    date = models.DateTimeField(null=True, blank=True)
    total_cases = models.IntegerField(null=True, blank=True)
    positive_cases = models.IntegerField(null=True, blank=True)
    is_outbreak = models.FloatField(null=True, blank=True)
    prediction = models.FloatField(null=True, blank=True)
    synced_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = "outbreak_predictions"

    def __str__(self):
        return f"{self.region_id} @ {self.date} -> {self.prediction}"


class FeatureImportance(models.Model):
    model_name = models.CharField(max_length=50)
    feature_name = models.CharField(max_length=100)
    importance = models.FloatField()
    trained_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = "feature_importance"

    def __str__(self):
        return f"{self.model_name} - {self.feature_name}: {self.importance}"