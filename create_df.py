from langchain_community.document_loaders import PDFMinerPDFasHTMLLoader

import datetime
import os

import pandas as pd

class create_df:
    def __init__(self):
        pass
    
    def create_df(filepath, last_id):
        
        filename = os.listdir(filepath)
        
        loader = PDFMinerPDFasHTMLLoader(os.path.join(filepath, filename[0]))
        
        # Separate files into multiple documents based on font size and semantics
        
        data = loader.load()[0]   # entire PDF is loaded as a single Document
        
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(data.page_content,'html.parser')
        content = soup.find_all('div')
        
        import re
        cur_fs = None
        cur_text = ''
        snippets = []   # first collect all snippets that have the same font size
        for c in content:
            sp = c.find('span')
            if not sp:
                continue
            st = sp.get('style')
            if not st:
                continue
            fs = re.findall('font-size:(\d+)px',st)
            if not fs:
                continue
            fs = int(fs[0])
            if not cur_fs:
                cur_fs = fs
            if fs == cur_fs:
                cur_text += c.text
            else:
                snippets.append((cur_text,cur_fs))
                cur_fs = fs
                cur_text = c.text
        snippets.append((cur_text,cur_fs))
        
        from langchain_community.docstore.document import Document
        cur_idx = -1
        semantic_snippets = []
        # Assumption: headings have higher font size than their respective content
        for s in snippets:
            # if current snippet's font size > previous section's heading => it is a new heading
            if not semantic_snippets or s[1] > semantic_snippets[cur_idx].metadata['heading_font']:
                metadata={'heading':s[0], 'content_font': 0, 'heading_font': s[1]}
                metadata.update(data.metadata)
                semantic_snippets.append(Document(page_content='',metadata=metadata))
                cur_idx += 1
                continue
        
            # if current snippet's font size <= previous section's content => content belongs to the same section (one can also create
            # a tree like structure for sub sections if needed but that may require some more thinking and may be data specific)
            if not semantic_snippets[cur_idx].metadata['content_font'] or s[1] <= semantic_snippets[cur_idx].metadata['content_font']:
                semantic_snippets[cur_idx].page_content += s[0]
                semantic_snippets[cur_idx].metadata['content_font'] = max(s[1], semantic_snippets[cur_idx].metadata['content_font'])
                continue
        
            # if current snippet's font size > previous section's content but less than previous section's heading than also make a new
            # section (e.g. title of a PDF will have the highest font size but we don't want it to subsume all sections)
            metadata={'heading':s[0], 'content_font': 0, 'heading_font': s[1]}
            metadata.update(data.metadata)
            semantic_snippets.append(Document(page_content='',metadata=metadata))
            cur_idx += 1
            
        import re
        
        # Extract document 5 content
        doc_5_content = semantic_snippets[5].page_content
        
        # Use regex to split by GRx headers
        sections_gr = re.split(r'(GR\d+ .+)', doc_5_content)
        
        # We now have headers and content alternating in the 'sections' list
        # We'll pair each header with its corresponding content
        split_snippets_gr = []
        for i in range(1, len(sections_gr), 2):
            header = sections_gr[i].strip()  # GR1, GR2, etc.
            content = sections_gr[i + 1].strip()  # Corresponding content
            split_snippets_gr.append((header, content))
            
        
        # Extract document 6 content
        doc_6_content = semantic_snippets[6].page_content
        
        # Use regex to split by PRx headers
        sections_pr = re.split(r'(PR\d+ .+)', doc_6_content)
        
        # We now have headers and content alternating in the 'sections' list
        # We'll pair each header with its corresponding content
        split_snippets_pr = []
        for i in range(1, len(sections_pr), 2):
            header = sections_pr[i].strip()  # PR1, PR2, etc.
            content = sections_pr[i + 1].strip()  # Corresponding content
            split_snippets_pr.append((header, content))
            
        final_documents = split_snippets_gr + split_snippets_pr
        
        # Convert final_documents into Document objects with metadata['requirement_code']
        documents_list = []
        
        for header, content in final_documents:
            # Create metadata for each document
            metadata = {
                'requirement_code': header,  # First item of the tuple is the requirement code
                'source': filename[0],       # Add the source metadata (filename)
                'date': datetime.datetime.now().date()  # Add the date metadata
            }
            
            # Create the Document object with the content and metadata
            doc = Document(page_content=content, metadata=metadata)
            
            # Add the document to the list
            documents_list.append(doc)
        
        docs_final_list = []
        
        for doc in documents_list:
            
            chunk = {
                "SOURCE" : doc.metadata['source'],
                "REQ_CODE" : doc.metadata['requirement_code'],
                "DATE" : doc.metadata['date'],
                "TEXT" : doc.page_content
                }
            
            docs_final_list.append(chunk)
            
        df = pd.DataFrame(docs_final_list)
        df['ID'] = range(last_id+ 1, last_id + len(df) + 1)
        
        df = df[["ID", "SOURCE", "REQ_CODE", "DATE", "TEXT"]]
        
        return df