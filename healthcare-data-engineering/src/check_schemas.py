from schemas import (
    patient_schema,
    encounter_schema,
    provider_schema,
    facility_schema,
    diagnosis_schema,
    medication_schema
)

print("Patients:")
print(patient_schema.simpleString())

print("\nEncounters:")
print(encounter_schema.simpleString())

print("\nProviders:")
print(provider_schema.simpleString())

print("\nFacilities:")
print(facility_schema.simpleString())

print("\nDiagnoses:")
print(diagnosis_schema.simpleString())

print("\nMedications:")
print(medication_schema.simpleString())