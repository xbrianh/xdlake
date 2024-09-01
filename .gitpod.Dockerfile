FROM gitpod/workspace-full

RUN sudo apt-get update && sudo apt-get install -y tmux fzf

# install the AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \ 
    && unzip awscliv2.zip \
    && sudo ./aws/install

# install the GCP CLI
RUN sudo apt-get update \
    && sudo apt-get install -y apt-transport-https ca-certificates gnupg curl \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && sudo apt-get update && sudo apt-get install -y google-cloud-cli

# install the azure CLI
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# set default shell to zsh
RUN sudo chsh -s /bin/zsh gitpod
