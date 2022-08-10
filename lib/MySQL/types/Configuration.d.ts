export declare type MySQLConfiguration = {
    host: string;
    user: string;
    password: string;
    database: string;
    charset: 'utf8mb4_general_ci';
    connectionLimit: number;
};
export declare type Configuration = {
    mysql: MySQLConfiguration;
};
