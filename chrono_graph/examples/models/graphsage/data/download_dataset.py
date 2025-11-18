from torch_geometric.data import download_url, extract_zip
from torch_geometric.data import HeteroData
import torch_geometric.transforms as T
import pandas as pd
import os
import torch

raw_data_directory = './MovieLens/Small/raw'

if not os.path.exists(raw_data_directory):
  url = 'https://files.grouplens.org/datasets/movielens/ml-latest-small.zip'
  extract_zip(download_url(url, '.'), '.')

  os.makedirs(raw_data_directory, exist_ok=True)
  os.remove('./ml-latest-small.zip')
  os.rename('./ml-latest-small', raw_data_directory)

movies_path = os.path.join(raw_data_directory, 'movies.csv')
ratings_path = os.path.join(raw_data_directory, 'ratings.csv')

# Load the entire movie data frame into memory:
movies_df = pd.read_csv(movies_path, index_col='movieId')

# Split genres and convert into indicator variables:
genres = movies_df['genres'].str.get_dummies('|')

# Use genres as movie input features:
movie_feat = torch.from_numpy(genres.values).to(torch.float)

# Load the entire ratings data frame into memory:
ratings_df = pd.read_csv(ratings_path)

# Create a mapping from unique user indices to range [0, num_user_nodes):
unique_user_id = ratings_df['userId'].unique()
unique_user_id = pd.DataFrame(data={
    'userId': unique_user_id,
    'mappedID': pd.RangeIndex(len(unique_user_id)),
})
# Create a mapping from unique movie indices to range [0, num_movie_nodes):
unique_movie_id = pd.DataFrame(data={
    'movieId': movies_df.index,
    'mappedID': pd.RangeIndex(len(movies_df)),
})

# Perform merge to obtain the edges from users and movies:
ratings_user_id = pd.merge(ratings_df['userId'], unique_user_id, left_on='userId', right_on='userId', how='left')
ratings_user_id = torch.from_numpy(ratings_user_id['mappedID'].values)
ratings_movie_id = pd.merge(ratings_df['movieId'], unique_movie_id, left_on='movieId', right_on='movieId', how='left')
ratings_movie_id = torch.from_numpy(ratings_movie_id['mappedID'].values)

# With this, we are ready to construct our `edge_index` in COO format
edge_index_user_to_movie = torch.stack([ratings_user_id, ratings_movie_id], dim=0)

data = HeteroData()

# Save node indices:
data["user"].node_id = torch.arange(len(unique_user_id))
data["movie"].node_id = torch.arange(len(movies_df))

# Add the node features and edge indices:
data["movie"].x = movie_feat
data["user", "rates", "movie"].edge_index = edge_index_user_to_movie

data = T.ToUndirected()(data)

print(data)

# Save the data object:
processed_data_directory = './MovieLens/Small/processed'
data_path = os.path.join(processed_data_directory, 'data.pt')
if not os.path.exists(processed_data_directory):
  print("Save data ...")
  os.makedirs(processed_data_directory, exist_ok=True)
  torch.save(data, data_path)
  print("Done")
