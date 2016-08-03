class DynamoDbResources:
    def resource(self, x, y):
        return {'table_names' :
                    {
                        'reg': 'prod1.app-installations.registrations.',
                        'kpis': 'prod1.app-installations.kpis.',
                        'clicks': 'prod1.ad-processor.banner-clicks.',
                        'app-conf': 'prod1.developer-api.app-configuration'
               },
               'table_index' :
                   {
                       'reg': 'deviceId-index',
                       'kpis_ixigo': 'campaignId-1stAppStartedDay-index',
                       'clicks': 'fingerprint-receivedAt-index'
                   },
               'conf' :
                   {
                       'exact_window': 'com.onesdk.click-attribution.retrospect-window.exact-match',
                       'fuzzy_window': 'com.onesdk.click-attribution.retrospect-window.fuzzy-match'
                   }
        }.get(x, {}).get(y)
