import os
from PIL import Image

from torch.utils.data import Dataset
import torchvision.transforms as transforms

img_transform = transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
        transforms.Normalize(
            mean=[0.485, 0.456, 0.406],
            std=[0.229, 0.224, 0.225])
    ])

class ImageDataset(Dataset):
    def __init__(self, root_dir, transform=None):
        self.root_dir = root_dir
        self.transform = transform
        self.img_names = os.listdir(self.root_dir)

    def __len__(self):
        return len(self.img_names)

    def __getitem__(self, idx):
        img_name = self.img_names[idx]
        img_path = os.path.join(self.root_dir, img_name)
        img = Image.open(img_path)
        if self.transform:
            img = self.transform(img)
        return img_name, img