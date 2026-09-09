export function selectModelCandidate(models, scopedModels, modelValue, providerValue, currentProvider) {
	if (!modelValue) return undefined;
	let provider = providerValue || currentProvider;
	let modelId = modelValue;
	const separator = modelValue.indexOf("/");
	const qualifiedProvider = separator > 0 ? modelValue.slice(0, separator) : null;
	if (qualifiedProvider && (!providerValue || qualifiedProvider === providerValue)) {
		provider = qualifiedProvider;
		modelId = modelValue.slice(separator + 1);
	}
	let model = provider ? models.find((candidate) => candidate.provider === provider && candidate.id === modelId) : undefined;
	if (model) return model;
	const matches = models.filter((candidate) => candidate.id === modelId);
	const scopedMatches = matches.filter((candidate) => scopedModels.length === 0 || scopedModels.some((entry) => {
		const scopedModel = entry?.model || entry;
		return scopedModel?.provider === candidate.provider && scopedModel?.id === candidate.id;
	}));
	return scopedMatches.length === 1 ? scopedMatches[0] : matches.length === 1 ? matches[0] : undefined;
}
