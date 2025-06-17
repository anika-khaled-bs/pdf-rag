import { Worker } from 'bullmq';
import { OpenAIEmbeddings } from "@langchain/openai";
import { QdrantVectorStore } from "@langchain/qdrant";
import { Document } from "@langchain/core/documents";
import { PDFLoader } from "@langchain/community/document_loaders/fs/pdf";
import { CharacterTextSplitter } from "@langchain/textsplitters";
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();



const worker = new Worker('file-upload-queue', async job => {
  try {
    console.log('Processing job:', job.data);
    const data = JSON.parse(job.data);

    //load the PDF file
    const loader = new PDFLoader(data.path);
    const docs = await loader.load();
    console.log('Loaded documents:', docs.length, 'documents');

    // const textSplitter = new CharacterTextSplitter({
    //   chunkSize: 300,
    //   chunkOverlap: 0,
    // });
    // const texts = await textSplitter.splitText(document);
    // console.log('splitted texts:', texts);
      
    console.log('Creating embeddings...');
    const embeddings = new OpenAIEmbeddings({
      model: 'text-embedding-3-small',
      apiKey: process.env.OPENAI_API_KEY,
    });

    console.log('Connecting to vector store...');
    const vectorStore = await QdrantVectorStore.fromExistingCollection(
      embeddings,
      {
        url: process.env.QDRANT_URL || 'http://localhost:6333',
        collectionName: process.env.QDRANT_COLLECTION || 'langchainjs-testing',
      }
    );
    
    console.log('Adding documents to vector store...');
    await vectorStore.addDocuments(docs);
    console.log(`All ${docs.length} docs are added to vector store`);

  } catch (error) {
    console.error('Error processing job:', error);
    console.error('Error details:', error.message);
    if (error.response) {
      console.error('API Response Error:', error.response.status, error.response.data);
    }
    throw error; // Re-throw to mark job as failed
  }
}, { concurrency: 100, connection: { host: process.env.REDIS_HOST || 'localhost', port: parseInt(process.env.REDIS_PORT) || 6379 } });

// Handle worker events
worker.on('completed', (job) => {
  console.log(`Job ${job.id} completed successfully`);
});

worker.on('failed', (job, err) => {
  console.log(`Job ${job.id} failed with error:`, err.message);
});

worker.on('error', (err) => {
  console.error('Worker error:', err);
});

console.log('Worker started with OpenAI embeddings...');