from mrjob.job import MRJob

# Descrição e valor por quilograma da commodity mais lucrativa por peso comercializada em 2015, 
# por tipo de fluxo. Considerar somente transações com Number of items, em que é possível calcular 
# o peso por unidade e o valor por quilo

# country_or_area;year;comm_code;commodity;flow;trade_usd;weight_kg;quantity_name;quantity;category

class CommodityComMaisLucroPorPeso(MRJob):
    def mapper(self, _, value):
        fields = value.split(';')
        if fields[0] == "country_or_area":
            return
        if len(fields) == 10: 
            year = fields[1]
            flow = fields[4]
            commodity = fields[3]
            quantityName = fields[7]
            
            if year == "2015" and quantityName == "Number of items":
                try:
                    tradeUsd = float(fields[5])
                    weightKg = float(fields[6])
                    if weightKg > 0: 
                        valuePerKg = tradeUsd / weightKg
                        yield flow, (commodity, valuePerKg)
                except ValueError:
                    pass

    def combiner(self, key, values):
        maxValue = 0
        commodityMaxValue = ""
        for commodity, value in values:
            if maxValue == 0 or value > maxValue:
                maxValue = value
                commodityMaxValue = commodity
        yield key, (commodityMaxValue, maxValue)

    def reducer(self, key, values):
        maxValue = 0
        commodityMaxValue = ""
        for commodity, value in values:
            if maxValue == 0 or value > maxValue:
                maxValue = value
                commodityMaxValue = commodity
        yield key, (commodityMaxValue, maxValue)

if __name__ == '__main__':
    CommodityComMaisLucroPorPeso.run()

# python .\Atividade8\Atividade8.py .\operacoes_comerciais_inteira.csv --output .\Atividade8\output\
# cat .\Atividade8\output\* > .\Atividade8\outputAtividade8