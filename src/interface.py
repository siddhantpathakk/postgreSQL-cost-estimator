import math
import gradio as gr

from explain import PGServer
from pydantic import BaseModel
from typing import Dict, List


class PGInterface(BaseModel):
    server: PGServer = PGServer()

    #Updates query display
    def updateQueryDisplay(self) -> list[gr.Markdown | gr.DataFrame]:
        output = self.server.execute_query()
        if len(output) != 2:
            return [gr.Markdown(output, visible=True), gr.DataFrame(visible=False)]
        else:   # Error message
            headers, data = output
            return [gr.Markdown(visible=False), gr.DataFrame(headers=headers, value=data, visible=True)]
        
    # Displays the QEP tree as a Plot.
    def displayQEPTree(self, query: str) -> list[gr.Markdown | gr.Plot]:
        options = ['ANALYZE', 'VERBOSE', 'COSTS', 'BUFFERS']    
        qep = self.server.get_qep(query, options)
        if isinstance(qep, str):    # Error message
            return [gr.Markdown(qep, visible=True), gr.Plot(visible=False)]
        
        figure = self.server.plot_qep_tree()
        msg = gr.Markdown(visible=False)
        plot = gr.Plot(figure, visible=True)
        return [msg, plot]
    
    #Creates layout of the interface, includes Connection, Query execution, QEP Tree and Query Output Table.
    def createInterface(self) -> None:
        # Connection 
        gr.Markdown('## Connection')
        with gr.Blocks():
            with gr.Row():
                host = gr.Textbox(label='Host', value='localhost')
                port = gr.Textbox(label='Port', value='5432')
                database = gr.Textbox(label='Database', value='TPC-H')
            with gr.Row():
                user = gr.Textbox(label='Username', value='postgres')
                password = gr.Textbox(
                    label='Password', placeholder='Enter password...', type='password', value='hi')
            with gr.Row():
                connectButton = gr.Button('Connect', variant='primary')
                disconnectButton = gr.Button('Disconnect', variant='stop')
            connectionStatus = gr.Markdown('Not connected')
            
            # When connected, show it in a green color box and disable the connect button
            gr.on(
                triggers=[connectButton.click, host.submit, port.submit,
                          database.submit, user.submit, password.submit],
                fn=self.server.connect,
                inputs=[host, port, database, user, password],
                outputs=connectionStatus
            )
            disconnectButton.click(
                self.server.disconnect, outputs=connectionStatus)

            
            
        # Execute Query
        gr.Markdown('## Query Execution')
        with gr.Blocks():
            with gr.Row():  
                with gr.Column():
                    query = gr.Textbox(
                        label='Query', lines=10, max_lines=10, placeholder='Input query here...')
                    
                    queryButton = gr.Button('Execute query')
                    
                with gr.Column():
                    with gr.Accordion('Open for examples', open=False):
                        gr.Examples(
                            inputs=query,
                            examples=[[sample] for sample in self.sampleQueries()]
                        )
        queryMessage = gr.Markdown(visible=False)

        #QEP Tree Visualization
        gr.Markdown('## QEP Tree')
        qepTreeMsg = gr.Markdown(
            'Enter a valid query in the textbox.')
        qepTree = gr.Plot(visible=False)

        #Query Output Table
        gr.Markdown('## Query Output Table')
        qepOutput = gr.DataFrame()

        queryButton.click(
            self.displayQEPTree, inputs=[query], outputs=[qepTreeMsg, qepTree]
        ).then(
            self.updateQueryDisplay, outputs=[queryMessage, qepOutput]
        )

    #Returns a list of sample queries for user's testing
    def sampleQueries(self) -> list[str]:
        queryList = [
            'select * from nation;',
            'select * from lineitem l inner join supplier s on l.l_suppkey = s.s_suppkey where l.l_suppkey = 10;',
            'select sum(l_tax * l_quantity) as totaltax from lineitem where l_quantity > 20;',
            '''
            select
                l_returnflag, l_linestatus, 
                sum(l_quantity) as total_quantity, 
                avg(l_extendedprice) as avg_price, 
                avg(l_discount) as avg_discount, 
                count(*) as count_orders
            from 
                lineitem
            where 
                l_shipdate <= DATE '1998-12-01' - INTERVAL '90 day'
            group by
                l_returnflag, l_linestatus
            order by
                l_returnflag, l_linestatus;
            ''',
            '''            
            select
                c_mktsegment,
                o_orderpriority,
                count(DISTINCT o_orderkey) as num_orders,
                sum(l_extendedprice * (1 - l_discount)) as revenue,
                avg(l_quantity) as avg_quantity,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_discount
            from 
                customer c
            join
                orders o ON c_custkey = o_custkey
            join
                lineitem l ON o_orderkey = l_orderkey
            where
                o_orderdate >= DATE '1995-01-01'
                AND o_orderdate < DATE '1995-01-01' + INTERVAL '3 month'
                AND l_shipdate > DATE '1995-01-01'
            group by 
                c_mktsegment, 
                o_orderpriority
            order by 
                revenue DESC, 
                c_mktsegment;
            ''',
            '''
            select avg(l.l_quantity) from lineitem l inner join supplier s on l.l_suppkey = s.s_suppkey where l.l_suppkey=10;
            ''',
            '''
            select
                supp_nation, cust_nation, l_year, sum(volume) as revenue
            from
                (
                    select
                        n1.n_name as supp_nation,
                        n2.n_name as cust_nation,
                        DATE_PART('YEAR',l_shipdate) as l_year,
                        l_extendedprice * (1 - l_discount) as volume
                    from
                        supplier, lineitem, orders, customer, nation n1, nation n2
                    where
                        s_suppkey = l_suppkey
                        and o_orderkey = l_orderkey
                        and c_custkey = o_custkey
                        and s_nationkey = n1.n_nationkey
                        and c_nationkey = n2.n_nationkey
                        and (
                            (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                            or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
                        )
                        and l_shipdate between '1995-01-01' and '1996-12-31'
                        and o_totalprice > 100
                        and c_acctbal > 10
                ) as shipping
            group by
                supp_nation, cust_nation, l_year
            order by
                supp_nation, cust_nation,l_year;
            '''
        ]
        return queryList
