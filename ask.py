from gen_ai_hub.proxy.langchain.init_models import init_llm
class ask:
    
    def __init__(self):
        pass
    
    def fetch_regulation(cc, regulation):
        
        sql_command_search = '''SELECT "ID", "TEXT" FROM VTSYKOLANOV.RI_MAIN_TABLE
        WHERE "REQ_CODE" = '{regulation}'
        ORDER BY TO_INT("ID") DESC
        LIMIT 1'''.format(regulation = regulation)

        hdf = cc.sql(sql_command_search)
        df_context = hdf.collect()

        context = df_context['TEXT'].str.cat(sep=' ')
        
        return context
    
    def get_regulation_info(cc, query, regulation):
                
        sql_command_search = '''SELECT "ID", "TEXT" FROM VTSYKOLANOV.RI_MAIN_TABLE
        WHERE "REQ_CODE" = '{regulation}'
        ORDER BY TO_INT("ID") DESC
        LIMIT 1'''.format(regulation = regulation)

        hdf = cc.sql(sql_command_search)
        df_context = hdf.collect()

        context = df_context['TEXT'].str.cat(sep=' ')

        prompt_start = (
            "Answer the question based on the context below.\n\n"+ 
            "Context: \n"
            )

        prompt_end = (
            f"\n\nQuestion: {query}\nAnswer:"
            )

        prompt = prompt_start + context + prompt_end

        llm = init_llm('gpt-4-32k', temperature=0.0, max_tokens=2000)
        
        return llm.invoke(prompt).content
    
    def compare_regulation(cc, query, regulation):
                
        sql_command_search = '''SELECT "ID", "TEXT" FROM VTSYKOLANOV.RI_MAIN_TABLE
        WHERE "REQ_CODE" = '{regulation}'
        ORDER BY TO_INT("ID") DESC
        LIMIT 1'''.format(regulation = regulation)

        hdf = cc.sql(sql_command_search)
        df_context = hdf.collect()

        context = df_context['TEXT'].str.cat(sep=' ')

        prompt_start = (
            "Compare new regulation based on the old regulation below. Explain what are the the differences in detail.\n\n"+ 
            "Context: \n"
            )

        prompt_end = (
            f"\n\nNew Regulation: {query}\nAnswer:"
            )

        prompt = prompt_start + context + prompt_end

        llm = init_llm('gpt-4-32k', temperature=0.0, max_tokens=2000)
        
        return llm.invoke(prompt).content