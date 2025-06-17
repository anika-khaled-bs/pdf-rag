import { Worker } from 'bullmq';
import { HuggingFaceTransformersEmbeddings } from "@langchain/community/embeddings/hf_transformers";
import { QdrantVectorStore } from "@langchain/qdrant";
import { PDFLoader } from "@langchain/community/document_loaders/fs/pdf";
import { CharacterTextSplitter } from "@langchain/textsplitters";

const worker = new Worker('file-upload-queue', async job => {
  try {
    console.log('Processing job:', job.data);
    const data = JSON.parse(job.data);

    // Load the PDF file
    const loader = new PDFLoader(data.path);
    const docs = await loader.load();
    console.log('Loaded documents:', docs.length, 'documents');

    // Optional: Split documents into smaller chunks
    const textSplitter = new CharacterTextSplitter({
      chunkSize: 1000,
      chunkOverlap: 200,
    });
    const splitDocs = await textSplitter.splitDocuments(docs);
    console.log('Split into', splitDocs.length, 'chunks');
      
    console.log('Creating local embeddings...');
    // Use HuggingFace local embeddings - no API key required!
    const embeddings = new HuggingFaceTransformersEmbeddings({
      model: "Xenova/all-MiniLM-L6-v2", // Small, fast model
    });

    console.log('Connecting to vector store...');
    const vectorStore = await QdrantVectorStore.fromExistingCollection(
      embeddings,
      {
        url: 'http://localhost:6333',
        collectionName: 'langchainjs-testing',
      }
    );
    
    console.log('Adding documents to vector store...');
    await vectorStore.addDocuments(splitDocs);
    console.log(`All ${splitDocs.length} document chunks are added to vector store`);

  } catch (error) {
    console.error('Error processing job:', error);
    console.error('Error details:', error.message);
    if (error.response) {
      console.error('API Response Error:', error.response.status, error.response.data);
    }
    throw error; // Re-throw to mark job as failed
  }
}, { concurrency: 1, connection: { host: 'localhost', port: 6379 } }); // Reduced concurrency for local processing

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

console.log('Worker started with local embeddings...');
