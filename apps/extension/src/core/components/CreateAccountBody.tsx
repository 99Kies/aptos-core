// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

import React, { useState } from 'react';
import { zxcvbnOptions } from '@zxcvbn-ts/core';
import { Button, useColorMode, VStack } from '@chakra-ui/react';
import { passwordOptions } from './CreatePasswordBody';
import SecretRecoveryPhraseBody from './SecretRecoveryPhraseBody';
import Copyable from './Copyable';

zxcvbnOptions.setOptions(passwordOptions);

interface Props {
  isLoading: boolean;
  mnemonic: string;
}

const buttonBorderColor = {
  dark: 'gray.700',
  light: 'gray.200',
};

export default function CreateAccountBody(
  { isLoading, mnemonic }: Props,
) {
  const { colorMode } = useColorMode();
  const [copied, setCopied] = useState<boolean>(false);

  return (
    <VStack spacing={4} display="flex" width="100%" height="100%" px={4}>
      <SecretRecoveryPhraseBody />
      <VStack width="100%" spacing={2} pb={3} borderTop="1px" pt={3} borderColor={buttonBorderColor[colorMode]}>
        <Copyable value={mnemonic} width="100%" copiedPrompt="">
          <Button
            width="100%"
            type="submit"
            isLoading={isLoading}
            px={8}
            onClick={() => setCopied(true)}
          >
            {copied ? 'Copied' : 'Copy'}
          </Button>
        </Copyable>
        <Button
          width="100%"
          colorScheme="teal"
          type="submit"
          isLoading={isLoading}
          px={8}
        >
          Create
        </Button>
      </VStack>
    </VStack>
  );
}