import torchvision
import torch.nn as nn

class ImageFeatureExtractor(nn.Module):
    def __init__(self):
        super().__init__()
        self.resnet = torchvision.models.resnet50(pretrained=True)
        self.resnet.fc = nn.Identity()

        for param in self.resnet.parameters():
            param.requires_grad = False

    def forward(self, x):
        return self.resnet(x)
