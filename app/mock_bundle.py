"""
mock_bundle.py
Generates a realistic FHIR R4 Bundle JSON string for UI simulation purposes.
The bundle contains one Patient, one Observation, and one Condition resource,
all using properly structured FHIR R4 fields.
"""

import json


def get_mock_bundle_dict() -> dict:
    """Return a realistic FHIR R4 Bundle as a Python dict."""
    return {
        "resourceType": "Bundle",
        "id": "ehr-migration-bundle-20240406",
        "meta": {
            "versionId": "1",
            "lastUpdated": "2024-04-06T14:32:00Z",
            "profile": [
                "http://hl7.org/fhir/StructureDefinition/Bundle"
            ]
        },
        "type": "collection",
        "timestamp": "2024-04-06T14:32:00Z",
        "total": 3,
        "entry": [
            {
                "fullUrl": "urn:uuid:patient-7f3a9c12-e4b1-4d2f-b8a0-1c5e6f7d8e90",
                "resource": {
                    "resourceType": "Patient",
                    "id": "patient-7f3a9c12-e4b1-4d2f-b8a0-1c5e6f7d8e90",
                    "meta": {
                        "profile": [
                            "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient"
                        ]
                    },
                    "identifier": [
                        {
                            "use": "usual",
                            "type": {
                                "coding": [
                                    {
                                        "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                        "code": "MR",
                                        "display": "Medical Record Number"
                                    }
                                ]
                            },
                            "system": "urn:oid:2.16.840.1.113883.4.3.25",
                            "value": "MRN-00482917"
                        }
                    ],
                    "active": True,
                    "name": [
                        {
                            "use": "official",
                            "family": "Marchetti",
                            "given": ["Elena", "Rose"]
                        }
                    ],
                    "gender": "female",
                    "birthDate": "1978-03-14",
                    "address": [
                        {
                            "use": "home",
                            "line": ["2847 Birchwood Drive"],
                            "city": "Atlanta",
                            "state": "GA",
                            "postalCode": "30301",
                            "country": "US"
                        }
                    ],
                    "telecom": [
                        {
                            "system": "phone",
                            "value": "404-555-0192",
                            "use": "home"
                        }
                    ]
                }
            },
            {
                "fullUrl": "urn:uuid:obs-b2c4d6e8-1a2b-3c4d-5e6f-7a8b9c0d1e2f",
                "resource": {
                    "resourceType": "Observation",
                    "id": "obs-b2c4d6e8-1a2b-3c4d-5e6f-7a8b9c0d1e2f",
                    "meta": {
                        "profile": [
                            "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-lab"
                        ]
                    },
                    "status": "final",
                    "category": [
                        {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                    "code": "vital-signs",
                                    "display": "Vital Signs"
                                }
                            ]
                        }
                    ],
                    "code": {
                        "coding": [
                            {
                                "system": "http://loinc.org",
                                "code": "8480-6",
                                "display": "Systolic blood pressure"
                            }
                        ],
                        "text": "Systolic blood pressure"
                    },
                    "subject": {
                        "reference": "urn:uuid:patient-7f3a9c12-e4b1-4d2f-b8a0-1c5e6f7d8e90",
                        "display": "Elena Rose Marchetti"
                    },
                    "effectiveDateTime": "2024-03-22T09:15:00Z",
                    "issued": "2024-03-22T09:30:00Z",
                    "valueQuantity": {
                        "value": 138,
                        "unit": "mmHg",
                        "system": "http://unitsofmeasure.org",
                        "code": "mm[Hg]"
                    },
                    "interpretation": [
                        {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
                                    "code": "H",
                                    "display": "High"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "fullUrl": "urn:uuid:cond-f1e2d3c4-b5a6-7890-abcd-ef1234567890",
                "resource": {
                    "resourceType": "Condition",
                    "id": "cond-f1e2d3c4-b5a6-7890-abcd-ef1234567890",
                    "meta": {
                        "profile": [
                            "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition"
                        ]
                    },
                    "clinicalStatus": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                                "code": "active",
                                "display": "Active"
                            }
                        ]
                    },
                    "verificationStatus": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                                "code": "confirmed",
                                "display": "Confirmed"
                            }
                        ]
                    },
                    "category": [
                        {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                                    "code": "problem-list-item",
                                    "display": "Problem List Item"
                                }
                            ]
                        }
                    ],
                    "code": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "38341003",
                                "display": "Hypertensive disorder"
                            },
                            {
                                "system": "http://hl7.org/fhir/sid/icd-10-cm",
                                "code": "I10",
                                "display": "Essential (primary) hypertension"
                            }
                        ],
                        "text": "Essential hypertension"
                    },
                    "subject": {
                        "reference": "urn:uuid:patient-7f3a9c12-e4b1-4d2f-b8a0-1c5e6f7d8e90",
                        "display": "Elena Rose Marchetti"
                    },
                    "onsetDateTime": "2019-07-10",
                    "recordedDate": "2019-07-10"
                }
            }
        ]
    }


def get_mock_bundle_json() -> str:
    """Return the mock FHIR R4 Bundle serialized as a formatted JSON string."""
    return json.dumps(get_mock_bundle_dict(), indent=2)
