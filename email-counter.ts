import { MongoClient, ReadPreference } from 'mongodb';
import dotenv from 'dotenv';
import express from 'express';
import cors from 'cors';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

// Get the directory name of the current module
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Load environment variables from .env file
dotenv.config();

interface EmailCounts {
  total_documents: number;
  actual_amount: number;
  enu_count: number;
  duplicate_emails: number;
  empty_emails: number;
}

interface RequestBody {
  database_name: string;
  collection_name: string;
}

async function countEmails(dbName: string, collectionName: string): Promise<EmailCounts> {
  const uri = process.env.MONGODB_URI;
  
  if (!uri) {
    throw new Error('MONGODB_URI environment variable is not set in .env file');
  }

  // Configure client with read-only options
  const client = new MongoClient(uri, {
    readPreference: ReadPreference.SECONDARY_PREFERRED, // Prefer reading from secondary nodes
    readConcern: { level: 'majority' }, // Ensure consistent reads
    retryReads: true, // Enable retry for read operations
    maxPoolSize: 1, // Limit connection pool
  });

  try {
    await client.connect();
    
    // Verify we have read access
    const database = client.db(dbName);
    const collection = database.collection(collectionName);
    
    // Test read access without modifying data
    await collection.findOne({}, { projection: { _id: 1 } });

    // Get total document count
    const totalDocuments = await collection.countDocuments({});

    // Count unique emails excluding the locked email (read-only operation)
    const uniqueEmails = await collection.distinct('email', {
      email: { $ne: 'email_not_unlocked@domain.com' }
    }, { 
      readPreference: ReadPreference.SECONDARY_PREFERRED 
    });

    // Count locked emails (read-only operation)
    const lockedEmails = await collection.countDocuments({
      email: 'email_not_unlocked@domain.com'
    }, { 
      readPreference: ReadPreference.SECONDARY_PREFERRED 
    });

    // Count empty or null emails
    const emptyEmails = await collection.countDocuments({
      $or: [
        { email: { $exists: false } },
        { email: null },
        { email: '' }
      ]
    }, {
      readPreference: ReadPreference.SECONDARY_PREFERRED
    });

    // Find duplicate emails (emails that appear more than once)
    const duplicateEmailPipeline = [
      {
        $match: {
          email: { $ne: 'email_not_unlocked@domain.com' }
        }
      },
      {
        $group: {
          _id: '$email',
          count: { $sum: 1 }
        }
      },
      {
        $match: {
          count: { $gt: 1 }
        }
      }
    ];

    const duplicateEmailResults = await collection.aggregate(duplicateEmailPipeline).toArray();
    const duplicateEmails = duplicateEmailResults.length;

    return {
      total_documents: totalDocuments,
      actual_amount: uniqueEmails.length,
      enu_count: lockedEmails,
      duplicate_emails: duplicateEmails,
      empty_emails: emptyEmails
    };
  } finally {
    await client.close();
  }
}

// Create Express app
const app = express();
const port = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

// API endpoint
app.post('/api/count-emails', async (req, res) => {
  try {
    const { database_name, collection_name } = req.body as RequestBody;

    // Validate request body
    if (!database_name || !collection_name) {
      return res.status(400).json({
        error: 'Missing required fields: database_name and collection_name are required'
      });
    }

    const counts = await countEmails(database_name, collection_name);
    res.json(counts);
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'An unknown error occurred'
    });
  }
});

// Start server
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
}); 