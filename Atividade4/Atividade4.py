from mrjob.job import MRJob

# Valor médio das transações por ano.

# country_or_area;year;comm_code;commodity;flow;trade_usd;weight_kg;quantity_name;quantity;category

class ValorMedio(MRJob):
    def mapper(self, _, value):
        fields = value.split(';')
        if fields[0] == "country_or_area":
            return
        if len(fields) == 10: 
            year = fields[1]
            try: 
                tradeUsd = float(fields[5])
                yield year, (tradeUsd, 1)
            except ValueError:
                pass     
                

    def combiner(self, key, values):
        total_value = 0
        total_count = 0
        for value, count in values:
            total_value += value
            total_count += count
        yield key, (total_value, total_count)

    def reducer(self, key, values):
        total_value = 0
        total_count = 0
        for value, count in values:
            total_value += value
            total_count += count
        if total_count > 0:
            avg_value = total_value / total_count
            yield key, avg_value
    
if __name__ == "__main__":
    ValorMedio.run()
    
# python .\Atividade2\Atividade2.py .\operacoes_comerciais_inteira.csv --output .\Atividade2\output\
#  cat .\Atividade4\output\* > .\Atividade4\outputAtividade4