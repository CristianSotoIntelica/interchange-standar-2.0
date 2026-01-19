import os
import json


class Parameters:
    """Class for store parameters for Mastercard files"""
    def __init__(self, *args):
        super(Parameters, self).__init__(*args)

    def getdataelements(self)-> dict:
        """store dataelements configuration.

        Returns:
            dataelements (dict): Dictionary with dataelements configuration.
        """
        dataelements = {
            1: {"fixed": True, "length": 8},
            2: {
                "fixed": False,
                "length": 2,
            },
            3: {
                "fixed": True,
                "length": 6,
            },
            4: {
                "fixed": True,
                "length": 12,
            },
            5: {
                "fixed": True,
                "length": 12,
            },
            6: {
                "fixed": True,
                "length": 12,
            },
            9: {
                "fixed": True,
                "length": 8,
            },
            10: {
                "fixed": True,
                "length": 8,
            },
            12: {
                "fixed": True,
                "length": 12,
            },
            14: {
                "fixed": True,
                "length": 4,
            },
            22: {
                "fixed": True,
                "length": 12,
            },
            23: {
                "fixed": True,
                "length": 3,
            },
            24: {
                "fixed": True,
                "length": 3,
            },
            25: {
                "fixed": True,
                "length": 4,
            },
            26: {
                "fixed": True,
                "length": 4,
            },
            30: {
                "fixed": True,
                "length": 24,
            },
            31: {
                "fixed": False,
                "length": 2,
            },
            32: {
                "fixed": False,
                "length": 2,
            },
            33: {
                "fixed": False,
                "length": 2,
            },
            37: {
                "fixed": True,
                "length": 12,
            },
            38: {
                "fixed": True,
                "length": 6,
            },
            40: {
                "fixed": True,
                "length": 3,
            },
            41: {
                "fixed": True,
                "length": 8,
            },
            42: {
                "fixed": True,
                "length": 15,
            },
            43: {
                "fixed": False,
                "length": 2,
            },
            48: {
                "fixed": False,
                "length": 3,
            },
            49: {
                "fixed": True,
                "length": 3,
            },
            50: {
                "fixed": True,
                "length": 3,
            },
            51: {
                "fixed": True,
                "length": 3,
            },
            54: {
                "fixed": False,
                "length": 3,
            },
            55: {
                "fixed": False,
                "length": 3,
            },
            62: {
                "fixed": False,
                "length": 3,
            },
            63: {
                "fixed": False,
                "length": 3,
            },
            71: {
                "fixed": True,
                "length": 8,
            },
            72: {
                "fixed": False,
                "length": 3,
            },
            73: {
                "fixed": True,
                "length": 6,
            },
            93: {
                "fixed": False,
                "length": 2,
            },
            94: {
                "fixed": False,
                "length": 2,
            },
            95: {
                "fixed": False,
                "length": 2,
            },
            100: {
                "fixed": False,
                "length": 2,
            },
            105: {
                "fixed": False,
                "length": 3,
            },
            111: {
                "fixed": False,
                "length": 3,
            },
            123: {
                "fixed": False,
                "length": 3,
            },
            124: {
                "fixed": False,
                "length": 3,
            },
            125: {
                "fixed": False,
                "length": 3,
            },
            127: {
                "fixed": False,
                "length": 3,
            },
        }

        return dataelements

    def getIPMParameters(self)->dict:
        """stores IPM tables parameters
        
        Returns:
           params (dict): Dictionary with structure for IPM tables read 
        
        """

        params = {
            "update_header": {
                "header": {
                    "header_title": {"start": 0, "end": 15},
                    "header_date": {"start": 15, "end": 23},
                    "header_time": {"start": 23, "end": 28},
                }
            },
            "replace_header": {
                "header": {
                    "header_title": {"start": 0, "end": 17},
                    "header_date": {"start": 45, "end": 54},
                    "header_time": {"start": 61, "end": 69},
                }
            },
            "key": {
                "layout": "IP0000T1",
                "key": {"start": 11, "end": 19},
                "table_ipm_id": {"start": 19, "end": 27},
                "table_sub_id": {"start": 243, "end": 246},
            },
            "record": {"start": 8, "end": 11},
            "tables": {
                "IP0040T1": {
                    "effective_timestamp": {"start": 0, "end": 7},
                    "active_inactive_code": {"start": 7, "end": 8},
                    "table_id": {"start": 8, "end": 11},
                    "low_range": {"start": 11, "end": 30,"data_type":"int64"},  # part_of_key
                    "gcms_product": {"start": 30, "end": 33},  # part_of_key
                    "high_range": {"start": 33, "end": 52,"data_type":"int64"},
                    "card_program_identifier": {"start": 52, "end": 55},
                    "card_program_priority": {"start": 55, "end": 57},
                    "member_id": {"start": 57, "end": 68},
                    "product_type": {"start": 68, "end": 69},
                    "endpoint": {"start": 69, "end": 76},
                    "card_country_alpha": {"start": 76, "end": 79},
                    "card_country_numeric": {"start": 79, "end": 82},
                    "region": {"start": 82, "end": 83},
                    "product_class": {"start": 83, "end": 86},
                    "tran_routing_ind": {"start": 86, "end": 87},
                    "first_present_reassign_ind": {"start": 87, "end": 88},
                    "product_reassign_switch": {"start": 88, "end": 89},
                    "pwcb_optin_switch": {"start": 89, "end": 90},
                    "licensed_product_id": {"start": 90, "end": 93},
                    "mapping_service_ind": {"start": 93, "end": 94},
                    "alm_participation_ind": {"start": 94, "end": 95},
                    "alm_activation_date": {"start": 95, "end": 101},
                    "cardholder_billing_currency_default": {"start": 101, "end": 104},
                    "cardholder_billing_currency_exponent_default": {
                        "start": 104,
                        "end": 105,
                    },
                    "cardholder_billing_primary_currency": {"start": 105, "end": 133},
                    "chip_to_magnetic": {"start": 133, "end": 134},
                    "floor_expiration_date": {"start": 134, "end": 140},
                    "co_brand_participation_switch": {"start": 140, "end": 141},
                    "spend_control_switch": {"start": 141, "end": 142},
                    "merchant_cleansing_service": {"start": 142, "end": 145},
                    "merchant_cleansing_activation": {"start": 145, "end": 151},
                    "contactless_enabled_indicator": {"start": 151, "end": 152},
                    "regulated_rate_type": {"start": 152, "end": 153},
                    "psn_route_indicator": {"start": 153, "end": 154},
                    "cashback_without_purchase_indicator": {"start": 154, "end": 155},
                    # "filler_1":{"start":155,"end":156},
                    "repower_reload_participation_indicator": {
                        "start": 156,
                        "end": 157,
                    },
                    "moneysend_indicator": {"start": 157, "end": 158},
                    "durbin_regulated_rate_indicator": {"start": 158, "end": 159},
                    "cash_access_only_participating_indicator": {
                        "start": 159,
                        "end": 160,
                    },
                    "authenticator_indicator": {"start": 160, "end": 161},
                    # "filler_2":{"start":161,"end":162},
                    "issuer_target_market_participation_indicator": {
                        "start": 162,
                        "end": 163,
                    },
                    "post_date_service_indicator": {"start": 163, "end": 164},
                    "meal_voucher_indicator": {"start": 164, "end": 165},
                    "non_reloadable_prepaid_switch": {"start": 165, "end": 167},
                    "faster_funds_indicator": {"start": 167, "end": 168},
                    "anonymous_prepaid_indicator": {"start": 168, "end": 169},
                    "cardholder_currency_indicator": {"start": 169, "end": 170},
                    "pay_by_account_indicator": {"start": 170, "end": 171},
                    "issuer_account_range_gaming_participation_indicator":{"start":171,"end":172},
                }
            },
        }

        return params
