from factual import Factual


class FactualClient():
    """Perform factual api requests"""
    # http://developer.factual.com/working-with-categories/

    factual = None

    def __init__(self, key, secret):
        self.factual = Factual(key, secret)

    def get_city_filter(self, d):
        filter = {'$and': []}
        for k, v in d.items():
            filter['$and'].append({k: {'$eq': v}})
        return filter

    def get_total_count(self, filter):
        return (self.factual
                .table('places')
                .filters(filter)
                .include_count(True)
                .total_row_count())

    def get_data(self, filter):
        return (self.factual
                .table('places')
                .filters(filter)
                .data())

    def get_category_count(self, cityquery, categoryIds):
        filter = self.get_city_filter(cityquery)
        filter['$and'].append({'category_ids': {'$includes_any': categoryIds}})
        return self.get_total_count(filter)

    # 29    Community and Government > Education > Colleges and Universities
    def get_college_count(self, cityquery):
        return self.get_category_count(cityquery, [29])

    # 181   Businesses and Services > Metals
    # 183   Businesses and Services > Petroleum
    # 184   Businesses and Services > Plastics
    # 186   Businesses and Services > Rubber
    # 190   Businesses and Services > Textiles
    # 192   Businesses and Services > Welding
    # 207   Businesses and Services > Automation and Control Systems
    # 208   Businesses and Services > Chemicals and Gasses
    # 213   Businesses and Servicess > Engineering
    # 268   Businesses and Services > Leather
    # 275   Businesses and Services > Manufacturing
    # 301   Businesses and Services > Renewable Energy
    # 447   Businesses and Services > Construction
    # 460   Businesses and Services > Technology
    def get_industry_count(self, cityquery):
        ids = [181, 183, 184, 186, 190, 192, 207, 208, 213, 268, 275, 301, 447,
               460]
        return self.get_category_count(cityquery, ids)

    # 218   Businesses and Services > Financial > Banking and Finance > ATMs
    def get_atm_count(self, cityquery):
        return self.get_category_count(cityquery, [218])

    # 221   Businesses and Services > Financial > Banking and Finance
    def get_bank_count(self, cityquery):
        return self.get_category_count(cityquery, [221])
