\copy region  FROM 'postgres-qep-visualizer\data\tpc-h\region.csv' DELIMITER '|';

\copy nation  FROM 'postgres-qep-visualizer\data\tpc-h\nation.csv' DELIMITER '|';

\copy part  FROM 'postgres-qep-visualizer\data\tpc-h\part.csv' DELIMITER '|';

\copy supplier  FROM 'postgres-qep-visualizer\data\tpc-h\supplier.csv' DELIMITER '|';

\copy partsupp  FROM 'postgres-qep-visualizer\data\tpc-h\partsupp.csv' DELIMITER '|';

\copy customer  FROM 'postgres-qep-visualizer\data\tpc-h\customer.csv' DELIMITER '|';

\copy orders  FROM 'postgres-qep-visualizer\data\tpc-h\orders.csv' DELIMITER '|';

\copy lineitem  FROM 'postgres-qep-visualizer\data\tpc-h\lineitem.csv' DELIMITER '|';
