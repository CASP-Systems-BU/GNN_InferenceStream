import torch
import logging
from ts.torch_handler.base_handler import BaseHandler
import os

logger = logging.getLogger(__name__)

class TraceHandler(BaseHandler):

    def initialize(self, ctx):
        super().initialize(ctx)
        # check threads, higher intra threads would lead to slower inference
#         logger.info(f"Intra-op threads: {torch.get_num_threads()}")
#         logger.info(f"Inter-op threads: {torch.get_num_interop_threads()}")
        torch.set_num_threads(1)         # Intra-op threads
        torch.set_num_interop_threads(64) # Inter-op threads (experiment with 4, 8)
        # Ensure model is on GPU if available
        if torch.cuda.is_available():
            self.device = torch.device("cuda")
        else:
            self.device = torch.device("cpu")

        self.model.to(self.device)
        self.model.eval()

    def preprocess(self, data):
        """Preprocess incoming data and create tensors."""
        feats_list = []
        edge_idx_list = []

        for req in data:
            req_body = req["body"]  # Subgraph JSON

            features = torch.tensor(req_body["data"], dtype=torch.float)
            edge_index = torch.tensor(req_body["edge_index"], dtype=torch.long)

            feats_list.append(features)
            edge_idx_list.append(edge_index)

        return (feats_list, edge_idx_list)

    def inference(self, inputs):
        """Move inputs to the right device and run inference."""
        feats_list, edge_idx_list = inputs
        results = []

        for i in range(len(feats_list)):
            x = feats_list[i].to(self.device)  # Move features to GPU/CPU
            e = edge_idx_list[i].to(self.device)  # Move edges to GPU/CPU

            with torch.no_grad():
                out = self.model(x, e)

            # First node embedding
            first_embedding = out[0].detach().cpu().tolist()  # Move to CPU before returning
            results.append(first_embedding)

        return results

    def postprocess(self, inference_output):
        """Return the result list."""
        return inference_output
