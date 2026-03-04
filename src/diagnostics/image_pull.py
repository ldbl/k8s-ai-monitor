"""ImagePullBackOff diagnostic plugin."""
from kubernetes import client as k8s


class ImagePullDiagnostic:
    issue_type = "image-pull"

    def diagnose(self, core: k8s.CoreV1Api, pod) -> dict:
        images = []
        for c in (pod.spec.containers or []):
            images.append({
                "container": c.name,
                "image": c.image,
                "pull_policy": c.image_pull_policy or "default",
            })

        pull_secrets_info = []
        pull_secrets = pod.spec.image_pull_secrets or []
        for s in pull_secrets:
            entry = {"name": s.name}
            try:
                core.read_namespaced_secret(s.name, pod.metadata.namespace)
                entry["exists"] = True
            except k8s.ApiException as e:
                if e.status == 404:
                    entry["exists"] = False
                else:
                    entry["check_failed"] = True
            pull_secrets_info.append(entry)

        data = {"images": images}
        if pull_secrets_info:
            data["pull_secrets"] = pull_secrets_info
        elif not pull_secrets:
            data["pull_secrets_configured"] = False
        return data
