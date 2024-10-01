from mrjob.job import MRJob

# Número de transações por ano

class Transacoes(MRJob):
    def mapper(self, _, value):
        fields = value.split(';')
        if fields[0] == "country_or_area":
            return
        if len(fields) == 10: #ignorar cabecalho
            year = fields[1]
            yield year, 1
            

    def combiner(self, key, value):
        yield key, sum(value)

    def reducer(self, key, value):
        yield key, sum(value)

    
if __name__ == "__main__":
    Transacoes.run()
    
# python .\Atividade2\Atividade2.py .\operacoes_comerciais_inteira.csv --output .\Atividade2\output\
# cat .\Atividade2\output\* > .\Atividade2\output_completo