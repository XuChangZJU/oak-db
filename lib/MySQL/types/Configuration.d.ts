export type MySQLConfiguration = {
    host: string;
    user: string;
    password: string;
    database: string;
    charset: 'utf8mb4_general_ci';
    connectionLimit: number;
};
export type Configuration = {
    mysql: MySQLConfiguration;
};
