allow_k8s_contexts('cluster-admin@sbernauer-demo-oidc')

# If tilt_options.json exists read it and load the default_registry value from it
settings = read_json('tilt_options.json', default={})
registry = settings.get('default_registry', 'oci.stackable.tech/sandbox')

# Configure default registry either read from config file above, or with default value of "oci.stackable.tech/sandbox"
default_registry(registry)

# meta = read_json('nix/meta.json')
# operator_name = meta['operator']['name']

operator_name = 'trino-lb'

custom_build(
    registry + '/' + operator_name,
    'nix shell -f . crate2nix -c crate2nix generate && nix-build . -A docker --argstr dockerName "${EXPECTED_REGISTRY}/' + operator_name + '" && ./result/load-image | docker load',
    deps=['trino-lb', 'trino-lb-persistence', 'trino-lb-core', 'Cargo.toml', 'Cargo.lock', 'self_signed_certs', 'default.nix', 'nix'],
    ignore=['*.~undo-tree~'],
    # ignore=['result*', 'Cargo.nix', 'target', *.yaml],
    outputs_image_ref_to='result/ref',
)

helm = helm(
    'deploy/helm/' + operator_name,
    name=operator_name,
    namespace="trino-lb",
    set=[
        'image.repository=' + registry + '/' + operator_name,
    ],
)
k8s_yaml(helm)

# # Load the latest CRDs from Nix
# watch_file('result')
# if os.path.exists('result'):
#    k8s_yaml('result/crds.yaml')

# # Exclude stale CRDs from Helm chart, and apply the rest
# helm_crds, helm_non_crds = filter_yaml(
#    helm(
#       'deploy/helm/' + operator_name,
#       name=operator_name,
#       namespace="stackable-operators",
#       set=[
#          'image.repository=' + registry + '/' + operator_name,
#       ],
#    ),
#    api_version = "^apiextensions\\.k8s\\.io/.*$",
#    kind = "^CustomResourceDefinition$",
# )
# k8s_yaml(helm_non_crds)
