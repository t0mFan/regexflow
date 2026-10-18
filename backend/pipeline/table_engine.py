"""Execute an AI-selected, validated plan on Spark without evaluating code."""
from functools import partial
import re2
from pyspark.sql import functions as F, types as T
from .assistant import ClarificationRequired, validate_table_plan
from .engine import transform, matching_stats


def col(name):
    return F.col('`'+name.replace('`','``')+'`')


def predicate(where):
    value=where['value'];column=col(where['column']);op=where['operator']
    if op=='is_empty':return column.isNull() | (F.trim(column)=='')
    if op=='is_not_empty':return column.isNotNull() & (F.trim(column)!='')
    if op=='equals':return column==F.lit(value)
    if op=='not_equals':return column!=F.lit(value)
    if op=='contains':return column.contains(value)
    if op=='not_contains':return ~column.contains(value)
    if op=='matches':
        fn=F.udf(lambda text:bool(text is not None and re2.search(value,text)),T.BooleanType())
        return fn(column)
    number=column.cast('double');target=F.lit(float(value))
    return {'gt':lambda:number>target,'gte':lambda:number>=target,'lt':lambda:number<target,'lte':lambda:number<=target}[op]()


def extract_partition(rows, source, target, pattern, group):
    compiled=re2.compile(pattern)
    for row in rows:
        values=row.asDict();value=values[source]
        match=compiled.search(value) if value is not None else None
        values[target]=match.group(group) if match else None
        yield values


def execute_table_plan(frame, plan, checkpoint=None):
    validate_table_plan(plan,frame.columns)
    if plan['status']=='clarify':raise ClarificationRequired(plan['message'])
    output=frame;warnings=[];edits=[]
    for i,step in enumerate(plan['steps']):
        if checkpoint:checkpoint(i,len(plan['steps']))
        op=step['op']
        if op in {'replace','extract'}:
            columns=step.get('columns',[step.get('column')])
            try:stats=matching_stats(output,columns,op,{'pattern':step['pattern']})
            except ValueError as exc:raise ClarificationRequired(str(exc)) from exc
            warnings.extend(stats['warnings']);edits.append({'step':i+1,'operation':op,'matched_cells':stats['matched_cells']})
        if op=='replace':
            output=transform(output,step['columns'],'replace',{'pattern':step['pattern']},step['replacement'])
        elif op=='extract':
            source,target=step['column'],step['target']
            if target not in output.columns:output=output.withColumn(target,F.lit(None).cast('string'))
            fn=partial(extract_partition,source=source,target=target,pattern=step['pattern'],group=step['group'])
            output=output.sparkSession.createDataFrame(output.rdd.mapPartitions(fn),output.schema)
        elif op=='normalize':
            output=transform(output,step['columns'],'normalize',{'normalization':step['mode']},'')
        elif op in {'set','fill_empty'}:
            for name in step['columns']:
                expression=F.lit(step['value'])
                if op=='fill_empty':expression=F.when(col(name).isNull()|(F.trim(col(name))==''),expression).otherwise(col(name))
                output=output.withColumn(name,expression)
        elif op=='set_where':
            output=output.withColumn(step['column'],F.when(predicate(step['where']),F.lit(step['value'])).otherwise(col(step['column'])))
        elif op=='map_values':
            for name in step['columns']:
                expression=col(name)
                for mapping in step['mappings']:
                    expression=F.when(col(name)==F.lit(mapping['from']),F.lit(mapping['to'])).otherwise(expression)
                output=output.withColumn(name,expression)
        elif op=='rename':output=output.withColumnRenamed(step['column'],step['target'])
        elif op=='select':output=output.select(*[col(name) for name in step['columns']])
        elif op=='filter':output=output.filter(predicate(step['where']))
        elif op=='compute':
            value=col(step['column']).cast('double');amount=F.lit(float(step['operand']))
            if output.filter(col(step['column']).isNotNull()&(F.trim(col(step['column']))!='')&value.isNull()).limit(1).count():
                raise ClarificationRequired('The calculation column contains non-numeric values. Specify how to handle them.')
            expression={'add':lambda:value+amount,'subtract':lambda:value-amount,'multiply':lambda:value*amount,'divide':lambda:value/amount}[step['operator']]()
            output=output.withColumn(step['target'],expression.cast('string'))
        elif op=='concat':output=output.withColumn(step['target'],F.concat_ws(step['separator'],*[col(c) for c in step['columns']]))
        elif op=='deduplicate':output=output.dropDuplicates(step['columns'])
        elif op=='sort':
            value=col(step['column']).cast('double') if step['numeric'] else col(step['column'])
            output=output.orderBy(value.asc_nulls_last() if step['direction']=='asc' else value.desc_nulls_last())
    return output,{'warnings':list(dict.fromkeys(warnings)),'step_statistics':edits}
