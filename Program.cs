using System;
using System.Linq;
using MySql.Data.MySqlClient;
using System.Collections.Generic;

class Program
{
    static string connectionString = "server=localhost;database=normalization;user=normalization;password=mikkogwapo23;";
    static void Main()
    {
        try
        {
            using (MySqlConnection conn = new MySqlConnection(connectionString))
            {
                conn.Open();
                Console.WriteLine("User PRIVACY contacts!!");
                DisplayMenu();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(" Error: " + ex.Message);
        }
    }

    static void DisplayMenu()
    {
        while (true)
        {
            Console.WriteLine("\nSelect an option:");
            Console.WriteLine("(1). Insert Raw Data");
            Console.WriteLine("(2). View Raw Data");
            Console.WriteLine("(3). View 1NF");
            Console.WriteLine("(4). View 2NF");
            Console.WriteLine("(5). View 3NF");
            Console.WriteLine("(6). Exit");
            Console.Write("Enter choice: ");

            string choice = Console.ReadLine();

            switch (choice)
            {
                case "1":
                    while (!InsertUserContacts()) { }
                    break;
                case "2":
                    FetchAndDisplayRawData();
                    break;
                case "3":
                    FetchAndDisplay1NF();
                    break;
                case "4":
                    FetchAndDisplay2NF();
                    break;
                case "5":
                    FetchAndDisplay3NF();
                    break;
                case "6":
                    Console.WriteLine("\nExiting program.");
                    return;
                default:
                    Console.WriteLine("Invalid choice. Try again.");
                    break;
            }
        }
    }

    static bool InsertUserContacts()
    {
        Console.Write("\nEnter Batch Name: ");
        string batchName = Console.ReadLine();

        Console.Write("\nEnter Names (comma-separated): ");
        string nameInput = Console.ReadLine();

        Console.Write("Enter Emails (comma-separated): ");
        string emailInput = Console.ReadLine();

        Console.Write("Enter Phone Numbers (comma-separated): ");
        string phoneInput = Console.ReadLine();

        string[] names = nameInput.Split(',').Select(e => e.Trim()).ToArray();
        string[] emails = emailInput.Split(',').Select(e => e.Trim()).ToArray();
        string[] phones = phoneInput.Split(',').Select(p => p.Trim()).ToArray();

        using (MySqlConnection conn = new MySqlConnection(connectionString))
        {
            try
            {
                conn.Open();

                // Insert new user group
                int groupId;
                using (MySqlCommand cmd = new MySqlCommand("INSERT INTO user_group (group_name) VALUES (@GroupName); SELECT LAST_INSERT_ID();", conn))
                {
                    cmd.Parameters.AddWithValue("@GroupName", batchName);
                    groupId = Convert.ToInt32(cmd.ExecuteScalar());
                }

                // Insert users and their emails/phones
                foreach (var name in names)
                {
                    int userId;
                    using (MySqlCommand cmd = new MySqlCommand("INSERT INTO users (first_name, group_id) VALUES (@Name, @GroupId); SELECT LAST_INSERT_ID();", conn))
                    {
                        cmd.Parameters.AddWithValue("@Name", name);
                        cmd.Parameters.AddWithValue("@GroupId", groupId);
                        userId = Convert.ToInt32(cmd.ExecuteScalar());
                    }

                    foreach (var email in emails)
                    {
                        using (MySqlCommand cmd = new MySqlCommand("INSERT INTO emails (user_id, email) VALUES (@UserId, @Email);", conn))
                        {
                            cmd.Parameters.AddWithValue("@UserId", userId);
                            cmd.Parameters.AddWithValue("@Email", email);
                            cmd.ExecuteNonQuery();
                        }
                    }

                    foreach (var phone in phones)
                    {
                        using (MySqlCommand cmd = new MySqlCommand("INSERT INTO phones (user_id, phone_number) VALUES (@UserId, @Phone);", conn))
                        {
                            cmd.Parameters.AddWithValue("@UserId", userId);
                            cmd.Parameters.AddWithValue("@Phone", phone);
                            cmd.ExecuteNonQuery();
                        }
                    }
                }

                Console.WriteLine("User contacts saved successfully with batch.");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error inserting data: " + ex.Message);
                return false;
            }
        }
    }

    static void FetchAndDisplayRawData()
    {
        using (MySqlConnection conn = new MySqlConnection(connectionString))
        {
            Console.WriteLine("\nRAW DATA TABLE:\n");
            conn.Open();
            string query = @"
            SELECT 
                g.group_id,
                g.group_name,
                GROUP_CONCAT(DISTINCT u.first_name ORDER BY u.user_id) AS names, 
                GROUP_CONCAT(DISTINCT e.email ORDER BY e.email_id) AS emails, 
                GROUP_CONCAT(DISTINCT p.phone_number ORDER BY p.phone_id) AS phones
            FROM 
                user_group g
            JOIN
                users u ON g.group_id = u.group_id
            LEFT JOIN 
                emails e ON u.user_id = e.user_id
            LEFT JOIN 
                phones p ON u.user_id = p.user_id
            GROUP BY 
                g.group_id
            ORDER BY 
                g.group_id;";

            ExecuteQuery(query);
        }
    }

    static void FetchAndDisplay1NF()
    {
        Console.WriteLine("\n1NF TABLE:\n");
        string query = @"
        SELECT 
            u.user_id,
            g.group_name,
            u.first_name, 
            e.email, 
            p.phone_number
        FROM 
            users u
        JOIN 
            user_group g ON u.group_id = g.group_id
        LEFT JOIN (
            SELECT 
                email_id, 
                user_id, 
                email, 
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY email_id) AS rn
            FROM 
                emails
        ) e ON u.user_id = e.user_id
        LEFT JOIN (
            SELECT 
                phone_id, 
                user_id, 
                phone_number, 
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY phone_id) AS rn
            FROM 
                phones
        ) p ON u.user_id = p.user_id AND e.rn = p.rn
        ORDER BY 
            g.group_name, u.user_id, e.rn;";

        ExecuteQuery(query);
    }

    static void FetchAndDisplay2NF()
    {
        Console.WriteLine("\n2NF TABLE:");
        Console.WriteLine("\nEMAIL TABLE:");
        string emailQuery = @"
        SELECT 
            e.email_id AS EmailID,
            u.group_id AS GroupID,
            u.first_name AS Name,
            e.email AS Email
        FROM 
            emails e
        JOIN 
            users u ON e.user_id = u.user_id
        ORDER BY 
            e.email_id;";

        ExecuteQuery(emailQuery);

        Console.WriteLine("\nPHONE TABLE:");
        string phoneQuery = @"
        SELECT 
            p.phone_id AS PhoneID,
            u.group_id AS GroupID,
            u.first_name AS Name,
            p.phone_number AS PhoneNumber
        FROM 
            phones p
        JOIN 
            users u ON p.user_id = u.user_id
        ORDER BY 
            p.phone_id;";

        ExecuteQuery(phoneQuery);
    }

    static void FetchAndDisplay3NF()
    {
        Console.WriteLine("\n3NF TABLE:");

        Console.WriteLine("\nUSER GROUP TABLE:");
        ExecuteQuery("SELECT group_id, group_name FROM user_group;");

        Console.WriteLine("\nUSERS TABLE:");
        ExecuteQuery("SELECT user_id, group_id, first_name FROM users;");

        Console.WriteLine("\nEMAILS TABLE:");
        ExecuteQuery("SELECT email_id, user_id, email FROM emails;");

        Console.WriteLine("\nPHONES TABLE:");
        ExecuteQuery("SELECT phone_id, user_id, phone_number FROM phones;");
    }

    static void ExecuteQuery(string query)
    {
        using (MySqlConnection conn = new MySqlConnection(connectionString))
        {
            conn.Open();
            using (MySqlCommand cmd = new MySqlCommand(query, conn))
            using (MySqlDataReader reader = cmd.ExecuteReader())
            {
                var columnNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
                var columnWidths = columnNames.Select(name => name.Length).ToArray();
                var rows = new List<string[]>();

                while (reader.Read())
                {
                    var row = new string[reader.FieldCount];
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        string value = reader.IsDBNull(i) ? "NULL" : reader.GetValue(i).ToString();
                        row[i] = value;
                        if (value.Length > columnWidths[i]) columnWidths[i] = value.Length;
                    }
                    rows.Add(row);
                }

                PrintRow(columnNames, columnWidths);
                PrintSeparator(columnWidths);
                foreach (var row in rows) PrintRow(row, columnWidths);
            }
        }
    }

    static void PrintRow(string[] row, int[] columnWidths) { for (int i = 0; i < row.Length; i++) Console.Write($"| {row[i].PadRight(columnWidths[i])} "); Console.WriteLine("|"); }

    static void PrintSeparator(int[] columnWidths) { foreach (var width in columnWidths) Console.Write("+" + new string('-', width + 2)); Console.WriteLine("+"); }
}
